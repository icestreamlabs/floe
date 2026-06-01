use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};

use ahash::AHashMap;
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
use futures::future::BoxFuture;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
use crate::dbsp_plan::{
    DbspProjectNode, DbspSelectNode, DbspSourceNode, ValidatedPlan, validate_dbsp_plan,
};
use crate::encoding::{
    EncodedRowProjectionColumn, EncodedRowProjectionSource, concat_encoded_rows,
    extract_encoded_row_columns,
};
use crate::materialized_view::MaterializedViewRegistry;
use crate::outer_stream::TransientSourceHandleStream;
use crate::task_events::{GraphTaskSender, report_graph_task_error};
use crate::vectorized_keys::{VectorizedEncodedKeyExtractor, VectorizedKeyedTimeBatch};

use super::compile::{
    PrekeyedIncrementalAggregateBatchEvaluator, build_count_aggregate_slot_kinds,
    build_count_batch_row_evaluator, build_incremental_aggregate_batch_row_evaluator,
    build_incremental_aggregate_slot_kinds, build_prekeyed_incremental_aggregate_batch_evaluator,
};
use super::materialize::{DeltaTransformFn, TransientMaterializeBatch};
use super::persistence_policy::{
    PersistencePolicy, PersistencePolicyConfig, TransientSegmentSpec, TransientSegmentStep,
};
use super::vectorized_filter_project::{
    VectorizedFilterProjectEvaluator, required_encoded_input_columns,
};

type ClosedJoinKeyTransformFn = dyn Fn(Arc<Vec<(Vec<u8>, i64)>>) -> BoxFuture<'static, Result<Vec<(Vec<u8>, i64)>>>
    + Send
    + Sync
    + 'static;

/// Orchestrates compilation of a [`CircuitPlan`] into DBSP streams backed by SlateDB.
pub struct DbspGraphBuilder {
    pub(super) bridge: Arc<Mutex<DbspBridge>>,
    ns: GraphNamespace,
    pub(super) watermark: Arc<AtomicI64>,
    pub(super) mv_flush_coalescing: MvFlushCoalescingConfig,
    pub(super) mv_overlay_snapshot: OverlaySnapshotConfig,
    persistence_policy_config: PersistencePolicyConfig,
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
            max_pending_rows: 4_000_000,
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
            mv_flush_coalescing: MvFlushCoalescingConfig::default(),
            mv_overlay_snapshot: OverlaySnapshotConfig::default(),
            persistence_policy_config: PersistencePolicyConfig::default(),
        })
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

    pub fn set_persistence_policy_config(&mut self, config: PersistencePolicyConfig) {
        self.persistence_policy_config = config;
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
        let persistence_policy =
            PersistencePolicy::for_plan_with_config(inputs.plan, self.persistence_policy_config);
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

        let identity_transform = identity_delta_transform();
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
                    let next = transient_topn::build_transient_topn_receiver_from_batches(
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
                self.compile_window_aggregate(window, upstream, append_only_input, task_events)
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

fn should_compact_transient_helper_state(
    upstream: &TransientSourceHandleStream,
    state_table: Option<&Arc<dyn KeyValueTable>>,
) -> bool {
    // Compact snapshots rewrite all helper state on every input batch. Keep that
    // behavior opt-in so the steady-state path persists only incremental deltas.
    upstream.recoverable() && state_table.is_some() && transient_compact_helper_state_env_enabled()
}

fn transient_compact_helper_state_env_enabled() -> bool {
    std::env::var("FLOE_COMPACT_TRANSIENT_HELPER_STATE")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
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
    transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<TransientWindowCountOutputProjection>,
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
    key: Arc<[u8]>,
}

#[derive(Clone, Copy)]
enum TransientWindowCountOutputProjection {
    GroupKeyAndCount,
}

enum TransientWindowCountUpdates {
    Full(AHashMap<(TransientWindowCountKey, i64), i64>),
    GroupKeyAndCount(AHashMap<(Arc<[u8]>, i64), i64>),
}

impl TransientWindowCountUpdates {
    fn new(projection: Option<TransientWindowCountOutputProjection>) -> Self {
        match projection {
            Some(TransientWindowCountOutputProjection::GroupKeyAndCount) => {
                Self::GroupKeyAndCount(AHashMap::new())
            }
            None => Self::Full(AHashMap::new()),
        }
    }

    fn merge(&mut self, key: &TransientWindowCountKey, count: i64, delta: i64) {
        if delta == 0 {
            return;
        }
        match self {
            Self::Full(updates) => {
                merge_i64_delta(updates, (key.clone(), count), delta);
            }
            Self::GroupKeyAndCount(updates) => {
                merge_i64_delta(updates, (Arc::clone(&key.key), count), delta);
            }
        }
    }
}

#[derive(Clone)]
enum TransientJoinPipelineStep {
    Transform(Arc<DeltaTransformFn>),
    Aggregate(DbspAggregateNode),
    TopN(DbspTopNNode),
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
    source_batch_journal_root_sources, source_batch_journal_root_sources_with_config,
    transient_source_root_requirements,
};

mod row_helpers;
mod source_requirements;
mod transient_receivers;
mod transient_segment;
mod transient_state;
mod transient_topn;

use row_helpers::*;
use transient_receivers::{build_transient_source_receiver, build_transient_transform_receiver};
use transient_segment::try_build_transient_segment_optimization;
use transient_state::PersistentTransientInputState;

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
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let transformed = match transform(Arc::clone(&batch.deltas)).await {
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
                            Some(transform) => match transform(Arc::clone(&batch.deltas)).await {
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
    let key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            select.output_schema().to_arrow_schema(),
            Arc::clone(&closed_key_columns),
        )
        .context("build vectorized transient closed-key extractor")?,
    );
    Ok(Some(Arc::new(move |delta_values| {
        let filter_transform = Arc::clone(&filter_transform);
        let key_extractor = Arc::clone(&key_extractor);
        Box::pin(async move {
            let selected = filter_transform(Arc::clone(&delta_values)).await?;
            let mut selected_keys = BTreeSet::new();
            for (key, _row, weight) in key_extractor.extract_keyed_deltas(&selected)? {
                if weight <= 0 {
                    continue;
                }
                selected_keys.insert(key);
            }

            let mut closed = BTreeMap::new();
            for (key, _row, weight) in key_extractor.extract_keyed_deltas(delta_values.as_ref())? {
                if weight <= 0 {
                    continue;
                }
                if selected_keys.contains(&key) {
                    continue;
                }
                *closed.entry(key).or_insert(0_i64) += weight;
            }
            Ok(closed.into_iter().collect())
        })
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
            TransientSourceRootShape::Source { .. } => Ok(identity_delta_transform()),
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
            transient_topn::fold_topn_root_output_projection(&mut shape);
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
                transient_topn::fold_topn_root_output_projection(&mut shape);
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
                        transient_topn::build_direct_projection_transform(
                            columns,
                            Arc::clone(project.input_schema()),
                        ),
                    );
                }
            } else {
                transient_topn::fold_topn_root_output_projection(&mut shape);
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
                transform: identity_delta_transform(),
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
                transform: None,
                output_projection: None,
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
            fold_window_count_star_output_projection(&mut shape)?;
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
                let Some(mut shape) = try_build_transient_source_window_count_star_root_shape(
                    plan,
                    select_input_idx,
                )?
                else {
                    return Ok(None);
                };
                fold_window_count_star_output_projection(&mut shape)?;
                shape.transform = compose_optional_delta_transform(
                    shape.transform.take(),
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
            if let Some(columns) = try_build_direct_row_projection(project)
                && shape.transform.is_none()
                && shape.output_projection.is_none()
                && try_build_window_count_group_key_count_projection(
                    columns.as_ref(),
                    shape.window.aggregate.group_keys().len(),
                )
                .is_some()
            {
                shape.output_projection =
                    Some(TransientWindowCountOutputProjection::GroupKeyAndCount);
            } else {
                fold_window_count_star_output_projection(&mut shape)?;
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
        shape.transform.clone(),
        shape.output_projection,
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

fn fold_window_count_star_output_projection(
    shape: &mut TransientSourceWindowCountStarRootShape,
) -> Result<()> {
    if let Some(output_projection) = shape.output_projection.take() {
        let transform = match output_projection {
            TransientWindowCountOutputProjection::GroupKeyAndCount => {
                let input_schema = transient_window_count_full_output_schema(&shape.window)?;
                let aggregate_width = shape.window.aggregate.output_schema().len();
                let columns = Arc::new((2..2 + aggregate_width).collect::<Vec<_>>());
                transient_topn::build_direct_projection_transform(columns, input_schema)
            }
        };
        shape.transform = compose_optional_delta_transform(shape.transform.take(), transform);
    }
    Ok(())
}

fn transient_window_count_full_output_schema(
    window: &dbsp::DbspWindowAggregateNode,
) -> Result<Arc<RowSchema>> {
    let mut fields = Vec::with_capacity(window.aggregate.output_schema().len() + 2);
    fields.push(dbsp::Field::new(
        "__floe_window_start",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.push(dbsp::Field::new(
        "__floe_window_end",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.extend(window.aggregate.output_schema().fields().iter().cloned());
    RowSchema::try_new(fields)
}

fn try_build_window_count_group_key_count_projection(
    columns: &[usize],
    group_key_count: usize,
) -> Option<TransientWindowCountOutputProjection> {
    if columns.len() != group_key_count + 1 {
        return None;
    }
    let count_column = group_key_count + 2;
    let expected_group_columns = 2..count_column;
    if columns[..group_key_count]
        .iter()
        .copied()
        .eq(expected_group_columns)
        && columns[group_key_count] == count_column
    {
        Some(TransientWindowCountOutputProjection::GroupKeyAndCount)
    } else {
        None
    }
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
                transform: identity_delta_transform(),
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
    let state_label = state_label.into();
    let compact_source_state =
        should_compact_transient_helper_state(&upstream, state_table.as_ref());
    tracing::info!(
        graph_id,
        state_label = %state_label,
        recoverable = upstream.recoverable(),
        helper_state_persistent = state_table.is_some(),
        compact_source_state,
        "configured transient aggregate helper state"
    );
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
    let debug_transient_join = tracing::enabled!(tracing::Level::DEBUG);
    if aggregate
        .aggregates()
        .iter()
        .all(|agg| agg.function() == &dbsp::DbspAggregateFunction::Count)
    {
        let slot_kinds = build_count_aggregate_slot_kinds(aggregate.aggregates());
        let row_evaluator = build_count_batch_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_count_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientCountAggregate::<Vec<u8>, Vec<u8>, Vec<u8>>::new_batch(
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
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
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
                        let final_deltas = match output_transform(Arc::new(encoded_output)).await {
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
                            deltas_consolidated: false,
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
        let row_evaluator = build_incremental_aggregate_batch_row_evaluator(
            Arc::clone(&aggregate_input_schema),
            aggregate.group_keys().to_vec(),
            aggregate.aggregates().to_vec(),
            Arc::clone(&aggregate_expression_columns),
            graph_id.clone(),
            "transient_aggregate",
        );
        let aggregate_processor = Arc::new(
            dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, Vec<u8>>::new_batch(
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
                            match evaluator
                                .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                                .await
                            {
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
                        let final_deltas = match output_transform(Arc::new(encoded_output)).await {
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
                            deltas_consolidated: false,
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
    output_transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<TransientWindowCountOutputProjection>,
    watermark: Arc<AtomicI64>,
    cancel: &CancellationToken,
    task_events: &GraphTaskSender,
    state_table: Option<Arc<dyn KeyValueTable>>,
    state_label: impl Into<String>,
) -> Result<mpsc::UnboundedReceiver<TransientMaterializeBatch>> {
    let state_label = state_label.into();
    let compact_count_state =
        should_compact_transient_helper_state(&upstream, state_table.as_ref());
    tracing::info!(
        graph_id,
        state_label = %state_label,
        recoverable = upstream.recoverable(),
        helper_state_persistent = state_table.is_some(),
        compact_count_state,
        "configured transient window count-star helper state"
    );
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
        output_projection,
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
    output_transform: Option<Arc<DeltaTransformFn>>,
    output_projection: Option<TransientWindowCountOutputProjection>,
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
        dbsp::DbspWindowPolicy::Session { .. } => {
            bail!("SESSION windows are not supported by the transient fixed-window receiver")
        }
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let track_evictions = allowed_lateness_ms != i64::MAX;
    let group_key_columns = Arc::new(group_key_columns);
    let window_key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            eval_schema.to_arrow_schema(),
            Arc::clone(&group_key_columns),
        )
        .context("build vectorized transient window count-star key extractor")?,
    );
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
        let mut counts: AHashMap<TransientWindowCountKey, i64> = AHashMap::new();
        let mut eviction_schedule: BTreeMap<i64, Vec<TransientWindowCountKey>> = BTreeMap::new();
        let restore_result = if compact_count_state {
            restore_transient_window_count_state(
                restored_deltas,
                &mut counts,
                &mut eviction_schedule,
                track_evictions,
            )
        } else {
            apply_transient_window_count_star_deltas(
                restored_deltas,
                window_key_extractor.as_ref(),
                time_column,
                window_size,
                window_slide,
                transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                None,
                &mut counts,
                &mut eviction_schedule,
                track_evictions,
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
                        match evaluator
                            .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                            .await
                        {
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
                        window_key_extractor.as_ref(),
                        time_column,
                        window_size,
                        window_slide,
                        transient_window_watermark_cutoff(&watermark, allowed_lateness_ms),
                        output_projection,
                        &mut counts,
                        &mut eviction_schedule,
                        track_evictions,
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
                    let final_deltas = if let Some(output_transform) = output_transform.as_ref() {
                        match output_transform(Arc::new(encoded_output)).await {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        }
                    } else {
                        encoded_output
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(final_deltas),
                        deltas_consolidated: output_transform.is_none(),
                    }).is_err() {
                        break;
                    }
                }
            }
        }
    });
    Ok(rx)
}

type PrecomputedWindowAggregateRows = VecDeque<dbsp::IncrementalAggregateRow<Vec<u8>>>;

fn build_transient_window_incremental_batches(
    keyed_time_batch: VectorizedKeyedTimeBatch,
    row_evaluator: &PrekeyedIncrementalAggregateBatchEvaluator,
    has_group_key: bool,
    window_size: i64,
    window_slide: i64,
    cutoff: Option<i64>,
    persist_inputs: bool,
) -> Result<(
    Vec<((Vec<u8>, Vec<u8>), i64)>,
    PrecomputedWindowAggregateRows,
    Vec<(Vec<u8>, i64)>,
)> {
    let mut windowed_deltas = Vec::new();
    let mut precomputed_rows = PrecomputedWindowAggregateRows::new();
    let mut persisted_window_rows = Vec::new();
    let mut encoded_window_cache: HashMap<(i64, i64), Vec<u8>> = HashMap::new();

    for delta in keyed_time_batch.deltas {
        if delta.diff == 0 || delta.event_ts < 0 {
            continue;
        }
        if let Some(cutoff) = cutoff
            && delta.event_ts < cutoff
        {
            continue;
        }

        let group_key = has_group_key.then_some(delta.key);
        let mut encoded_keys = Vec::new();
        let mut build_error: Option<anyhow::Error> = None;
        transient_window_for_each_window(
            delta.event_ts,
            window_size,
            window_slide,
            |window_start, window_end| {
                if build_error.is_some() {
                    return;
                }
                let encoded_window = match encoded_window_cache.get(&(window_start, window_end)) {
                    Some(encoded) => encoded.clone(),
                    None => match encode_transient_window_bounds(window_start, window_end) {
                        Ok(encoded) => {
                            encoded_window_cache
                                .insert((window_start, window_end), encoded.clone());
                            encoded
                        }
                        Err(err) => {
                            build_error = Some(err);
                            return;
                        }
                    },
                };
                let encoded_key = if let Some(group_key) = group_key.as_ref() {
                    match concat_encoded_rows(&encoded_window, group_key) {
                        Ok(encoded) => encoded,
                        Err(err) => {
                            build_error = Some(err);
                            return;
                        }
                    }
                } else {
                    encoded_window
                };
                encoded_keys.push(encoded_key);
            },
        );
        if let Some(err) = build_error {
            return Err(err);
        }
        if encoded_keys.is_empty() {
            continue;
        }

        let slots = row_evaluator.evaluate_batch_row(
            &keyed_time_batch.batch,
            &keyed_time_batch.input_positions,
            delta.batch_row,
        );
        let last_idx = encoded_keys.len() - 1;
        let mut row = Some(delta.row);
        for (idx, encoded_key) in encoded_keys.into_iter().enumerate() {
            let row_value = if idx == last_idx {
                row.take().expect("transient window row already moved")
            } else {
                row.as_ref().expect("transient window row missing").clone()
            };
            let slot_values = slots.clone();
            let pair = (encoded_key.clone(), row_value);
            if persist_inputs {
                let encoded = encode_transient_window_aggregate_input_pair(&pair.0, &pair.1)?;
                persisted_window_rows.push((encoded, delta.diff));
            }
            precomputed_rows.push_back(dbsp::IncrementalAggregateRow {
                key: encoded_key,
                slots: slot_values,
            });
            windowed_deltas.push((pair, delta.diff));
        }
    }

    Ok((windowed_deltas, precomputed_rows, persisted_window_rows))
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
    let state_label = state_label.into();
    let compact_source_state =
        should_compact_transient_helper_state(&upstream, state_table.as_ref());
    tracing::info!(
        graph_id,
        state_label = %state_label,
        recoverable = upstream.recoverable(),
        helper_state_persistent = state_table.is_some(),
        compact_source_state,
        "configured transient window aggregate helper state"
    );
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
        dbsp::DbspWindowPolicy::Session { .. } => {
            bail!("SESSION windows are not supported by the transient fixed-window receiver")
        }
    };
    let allowed_lateness_ms = window.window.allowed_lateness_ms;
    let slot_kinds = build_incremental_aggregate_slot_kinds(window.aggregate.aggregates())
        .ok_or_else(|| {
            anyhow!("window aggregate is not eligible for transient incremental aggregation")
        })?;
    let group_key_columns = Arc::new(group_key_columns);
    let window_key_extractor = Arc::new(
        VectorizedEncodedKeyExtractor::new(
            eval_schema.to_arrow_schema(),
            Arc::clone(&group_key_columns),
        )
        .context("build vectorized transient window key extractor")?,
    );
    let prekeyed_evaluator = Arc::new(build_prekeyed_incremental_aggregate_batch_evaluator(
        Arc::clone(&eval_schema),
        window.aggregate.aggregates().to_vec(),
        Arc::clone(&expression_columns),
        graph_id.to_string(),
        "transient_window_aggregate",
    ));
    let precomputed_rows = Arc::new(StdMutex::new(PrecomputedWindowAggregateRows::new()));
    let aggregate_processor = Arc::new(
        dbsp::DbspTransientIncrementalAggregate::<Vec<u8>, (Vec<u8>, Vec<u8>)>::new_batch(
            {
                let prekeyed_evaluator = Arc::clone(&prekeyed_evaluator);
                let precomputed_rows = Arc::clone(&precomputed_rows);
                move |delta_values: &[((Vec<u8>, Vec<u8>), i64)]| {
                    let mut evaluated = Vec::with_capacity(delta_values.len());
                    let mut misses = Vec::new();
                    match precomputed_rows.lock() {
                        Ok(mut precomputed) if precomputed.len() >= delta_values.len() => {
                            for (pair, weight) in delta_values {
                                if let Some(row) = precomputed.pop_front() {
                                    evaluated.push((pair.clone(), row, *weight));
                                } else {
                                    misses.push((pair.clone(), *weight));
                                }
                            }
                        }
                        Ok(_) | Err(_) => {
                            misses.extend(delta_values.iter().cloned());
                        }
                    }
                    if !misses.is_empty() {
                        evaluated.extend(prekeyed_evaluator.evaluate_deltas(&misses));
                    }
                    evaluated
                }
            },
            slot_kinds,
        )
        .await
        .context("initialize transient window incremental aggregate")?,
    );
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
                        match evaluator
                            .transform_delta_arrow(&graph_id, Arc::new(input_deltas))
                            .await
                        {
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
                    let keyed_time_batch = match window_key_extractor
                        .extract_keyed_time_batch_with_columns(
                            &input_deltas,
                            time_column,
                            prekeyed_evaluator.required_input_columns(),
                        ) {
                        Ok(batch) => batch,
                        Err(err) => {
                            report_graph_task_error(
                                &task_events,
                                &graph_id,
                                task_label.clone(),
                                err.context("extract vectorized transient window aggregate keys"),
                            );
                            break;
                        }
                    };
                    let (windowed_deltas, evaluated_rows, persisted_window_rows) =
                        match keyed_time_batch {
                            Some(batch) => match build_transient_window_incremental_batches(
                                batch,
                                prekeyed_evaluator.as_ref(),
                                !group_key_columns.is_empty(),
                                window_size,
                                window_slide,
                                cutoff,
                                !compact_source_state,
                            ) {
                                Ok(batches) => batches,
                                Err(err) => {
                                    report_graph_task_error(
                                        &task_events,
                                        &graph_id,
                                        task_label.clone(),
                                        err,
                                    );
                                    break;
                                }
                            },
                            None => (Vec::new(), PrecomputedWindowAggregateRows::new(), Vec::new()),
                        };
                    if !compact_source_state {
                        if let Err(err) = persistent_state.apply_deltas(&persisted_window_rows).await {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    }
                    let use_precomputed_rows =
                        windowed_deltas.iter().all(|(_, weight)| *weight >= 0);
                    if let Ok(mut precomputed) = precomputed_rows.lock() {
                        *precomputed = if use_precomputed_rows {
                            evaluated_rows
                        } else {
                            PrecomputedWindowAggregateRows::new()
                        };
                    }
                    let mut aggregate_deltas = match aggregate_processor.apply_deltas(windowed_deltas).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            if let Ok(mut precomputed) = precomputed_rows.lock() {
                                precomputed.clear();
                            }
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if let Ok(mut precomputed) = precomputed_rows.lock() {
                        precomputed.clear();
                    }
                    if let Some(cutoff) = cutoff {
                        let evicted = match aggregate_processor
                            .evict_keys_where(|key| match transient_window_encoded_key_end(key) {
                                Ok(end) => end <= cutoff,
                                Err(err) => {
                                    tracing::warn!(
                                        graph_id = %graph_id,
                                        error = %err,
                                        "skipping malformed transient window aggregate key during eviction"
                                    );
                                    false
                                }
                            })
                            .await
                        {
                            Ok(deltas) => deltas,
                            Err(err) => {
                                report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                                break;
                            }
                        };
                        merge_incremental_aggregate_output_deltas(&mut aggregate_deltas, evicted);
                    }
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
                    let final_deltas = match output_transform(Arc::new(encoded_output)).await {
                        Ok(deltas) => deltas,
                        Err(err) => {
                            report_graph_task_error(&task_events, &graph_id, task_label.clone(), err);
                            break;
                        }
                    };
                    if tx.send(TransientMaterializeBatch {
                        version: batch.version,
                        deltas: Arc::new(final_deltas),
                        deltas_consolidated: false,
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
    if matches!(window.window.policy, dbsp::DbspWindowPolicy::Session { .. }) {
        return false;
    }
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
    if matches!(window.window.policy, dbsp::DbspWindowPolicy::Session { .. }) {
        return false;
    }
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
    if allowed_lateness_ms == i64::MAX {
        return None;
    }
    let watermark = watermark.load(Ordering::Relaxed);
    if watermark < 0 {
        return None;
    }
    Some(watermark.saturating_sub(allowed_lateness_ms.max(0)))
}

fn merge_i64_delta<K, S>(map: &mut HashMap<K, i64, S>, key: K, delta: i64)
where
    K: Eq + Hash,
    S: BuildHasher,
{
    if delta == 0 {
        return;
    }

    match map.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let merged = entry.get().saturating_add(delta);
            if merged == 0 {
                entry.remove();
            } else {
                *entry.get_mut() = merged;
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(delta);
        }
    }
}

fn apply_transient_window_count_delta(
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    updates: &mut TransientWindowCountUpdates,
    key: TransientWindowCountKey,
    delta: i64,
    track_evictions: bool,
) {
    if delta == 0 {
        return;
    }
    let old_count = counts.get(&key).copied().unwrap_or(0);
    let new_count = old_count.saturating_add(delta);
    if old_count == new_count {
        return;
    }
    if old_count != 0 {
        updates.merge(&key, old_count, -1);
    }
    if new_count != 0 {
        updates.merge(&key, new_count, 1);
        if track_evictions && old_count == 0 {
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

fn transient_window_evict_expired_counts(
    cutoff: Option<i64>,
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    updates: &mut TransientWindowCountUpdates,
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
            updates.merge(&key, old_count, -1);
        }
    }
}

fn apply_transient_window_count_star_deltas(
    input_deltas: Vec<(Vec<u8>, i64)>,
    key_extractor: &VectorizedEncodedKeyExtractor,
    time_column: usize,
    window_size: i64,
    window_slide: i64,
    cutoff: Option<i64>,
    output_projection: Option<TransientWindowCountOutputProjection>,
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    track_evictions: bool,
) -> Result<TransientWindowCountUpdates> {
    let mut grouped_deltas: AHashMap<TransientWindowCountKey, i64> = AHashMap::new();
    let mut batch_group_key_intern: AHashMap<Vec<u8>, Arc<[u8]>> = AHashMap::new();
    for (_row, weight, raw_key, event_ts) in
        key_extractor.extract_keyed_time_deltas(&input_deltas, time_column)?
    {
        if weight == 0 {
            continue;
        }
        if event_ts < 0 {
            continue;
        }
        if let Some(cutoff) = cutoff
            && event_ts < cutoff
        {
            continue;
        }
        let key = match batch_group_key_intern.get(raw_key.as_slice()) {
            Some(key) => Arc::clone(key),
            None => {
                let key = Arc::<[u8]>::from(raw_key.clone().into_boxed_slice());
                batch_group_key_intern.insert(raw_key, Arc::clone(&key));
                key
            }
        };
        transient_window_for_each_window(event_ts, window_size, window_slide, |start, end| {
            merge_i64_delta(
                &mut grouped_deltas,
                TransientWindowCountKey {
                    start,
                    end,
                    key: Arc::clone(&key),
                },
                weight,
            );
        });
    }

    let mut updates = TransientWindowCountUpdates::new(output_projection);
    for (key, delta) in grouped_deltas {
        apply_transient_window_count_delta(
            counts,
            eviction_schedule,
            &mut updates,
            key,
            delta,
            track_evictions,
        );
    }

    if track_evictions {
        transient_window_evict_expired_counts(cutoff, counts, eviction_schedule, &mut updates);
    }
    Ok(updates)
}

fn encode_transient_window_count_state(
    counts: &AHashMap<TransientWindowCountKey, i64>,
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
    counts: &mut AHashMap<TransientWindowCountKey, i64>,
    eviction_schedule: &mut BTreeMap<i64, Vec<TransientWindowCountKey>>,
    track_evictions: bool,
) -> Result<()> {
    for (row, count) in rows {
        if count == 0 {
            continue;
        }
        let key = decode_transient_window_count_state_key(&row)?;
        counts.insert(key.clone(), count);
        if track_evictions {
            eviction_schedule.entry(key.end).or_default().push(key);
        }
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
    Ok(TransientWindowCountKey {
        start,
        end,
        key: Arc::<[u8]>::from(key.into_boxed_slice()),
    })
}

fn transient_window_encoded_key_end(row: &[u8]) -> Result<i64> {
    extract_encoded_row_i64_like_column(row, 1)?
        .ok_or_else(|| anyhow!("encoded window key end is null"))
}

fn encode_transient_window_count_output_deltas(
    deltas: TransientWindowCountUpdates,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let deltas = match deltas {
        TransientWindowCountUpdates::Full(deltas) => deltas,
        TransientWindowCountUpdates::GroupKeyAndCount(deltas) => {
            return encode_transient_window_count_group_key_count_output_deltas(deltas);
        }
    };
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

fn encode_transient_window_count_group_key_count_output_deltas(
    deltas: AHashMap<(Arc<[u8]>, i64), i64>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    let mut projected = Vec::with_capacity(deltas.len());
    for ((key, count), diff) in deltas {
        if diff == 0 {
            continue;
        }
        let row = encode_transient_window_group_key_count_output_row(&key, count)?;
        projected.push((row, diff));
    }
    Ok(projected)
}

fn encode_transient_window_group_key_count_output_row(
    group_key: &[u8],
    count: i64,
) -> Result<Vec<u8>> {
    if group_key.len() < 4 {
        bail!("transient window count group key is too short");
    }
    let group_key_count = transient_encoded_row_declared_column_count(group_key)?;
    let output_count = group_key_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("too many columns in MV key"))?;
    let output_count =
        u32::try_from(output_count).map_err(|_| anyhow!("too many columns in MV key"))?;
    let mut row = Vec::with_capacity(group_key.len() + 9);
    row.extend_from_slice(&output_count.to_le_bytes());
    row.extend_from_slice(&group_key[4..]);
    row.push(0x01);
    row.extend_from_slice(&count.to_le_bytes());
    Ok(row)
}

fn transient_encoded_row_declared_column_count(row: &[u8]) -> Result<usize> {
    if row.len() < 4 {
        bail!("encoded key too short");
    }
    Ok(u32::from_le_bytes(row[0..4].try_into().unwrap()) as usize)
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
const AGGREGATE_VALUE_NULL_DATE_DAYS_TAG: u8 = 7;
const AGGREGATE_VALUE_NULL_DECIMAL128_TAG: u8 = 8;
const AGGREGATE_VALUE_DATE_DAYS_TAG: u8 = 9;
const AGGREGATE_VALUE_DECIMAL128_TAG: u8 = 10;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_TAG: u8 = 1;
const INCREMENTAL_AGGREGATE_SLOT_COUNT_DISTINCT_TAG: u8 = 2;
const INCREMENTAL_AGGREGATE_SLOT_SUM_TAG: u8 = 3;
const INCREMENTAL_AGGREGATE_SLOT_AVG_TAG: u8 = 4;
const INCREMENTAL_AGGREGATE_SLOT_MIN_TAG: u8 = 5;
const INCREMENTAL_AGGREGATE_SLOT_MAX_TAG: u8 = 6;
const INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG: u8 = 7;

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
        dbsp::IncrementalAggregateSlotState::DecimalSum {
            sum,
            non_null_count,
        } => {
            dst.push(INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG);
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
        INCREMENTAL_AGGREGATE_SLOT_DECIMAL_SUM_TAG => {
            Ok(dbsp::IncrementalAggregateSlotState::DecimalSum {
                sum: read_i128_le(bytes, cursor)?,
                non_null_count: read_i64_le(bytes, cursor)?,
            })
        }
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
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::DateDays) => {
            dst.push(AGGREGATE_VALUE_NULL_DATE_DAYS_TAG);
        }
        dbsp::AggregateValue::Null(dbsp::AggregateValueType::Decimal128 { precision, scale }) => {
            dst.push(AGGREGATE_VALUE_NULL_DECIMAL128_TAG);
            dst.push(precision);
            dst.push(scale as u8);
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
        dbsp::AggregateValue::DateDays(value) => {
            dst.push(AGGREGATE_VALUE_DATE_DAYS_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
        }
        dbsp::AggregateValue::Decimal128(value) => {
            dst.push(AGGREGATE_VALUE_DECIMAL128_TAG);
            dst.extend_from_slice(&value.to_le_bytes());
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
        AGGREGATE_VALUE_NULL_DATE_DAYS_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::DateDays,
        )),
        AGGREGATE_VALUE_NULL_DECIMAL128_TAG => Ok(dbsp::AggregateValue::Null(
            dbsp::AggregateValueType::Decimal128 {
                precision: read_u8(bytes, cursor)?,
                scale: read_u8(bytes, cursor)? as i8,
            },
        )),
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
        AGGREGATE_VALUE_DATE_DAYS_TAG => {
            let end = cursor
                .checked_add(4)
                .ok_or_else(|| anyhow!("date-days cursor overflow"))?;
            if end > bytes.len() {
                bail!("truncated aggregate date-days value");
            }
            let value = i32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
            *cursor = end;
            Ok(dbsp::AggregateValue::DateDays(value))
        }
        AGGREGATE_VALUE_DECIMAL128_TAG => {
            let end = cursor
                .checked_add(16)
                .ok_or_else(|| anyhow!("decimal cursor overflow"))?;
            if end > bytes.len() {
                bail!("truncated aggregate decimal value");
            }
            let value = i128::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
            *cursor = end;
            Ok(dbsp::AggregateValue::Decimal128(value))
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

fn read_i128_le(bytes: &[u8], cursor: &mut usize) -> Result<i128> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| anyhow!("i128 cursor overflow"))?;
    let chunk = bytes
        .get(*cursor..end)
        .ok_or_else(|| anyhow!("truncated i128"))?;
    *cursor = end;
    Ok(i128::from_le_bytes(chunk.try_into().unwrap()))
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

fn merge_incremental_aggregate_output_deltas(
    target: &mut Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
    updates: Vec<((Vec<u8>, Vec<dbsp::AggregateValue>), i64)>,
) {
    if updates.is_empty() {
        return;
    }

    let mut merged = HashMap::<(Vec<u8>, Vec<dbsp::AggregateValue>), i64>::new();
    for (row, delta) in target.drain(..).chain(updates) {
        if delta == 0 {
            continue;
        }
        let entry = merged.entry(row.clone()).or_insert(0);
        *entry += delta;
        if *entry == 0 {
            merged.remove(&row);
        }
    }
    target.extend(merged);
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
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::DateDays) => encoded.push(0x0A),
            dbsp::AggregateValue::Null(dbsp::AggregateValueType::Decimal128 { .. }) => {
                encoded.push(0x0C);
            }
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
            dbsp::AggregateValue::DateDays(value) => {
                encoded.push(0x09);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            dbsp::AggregateValue::Decimal128(value) => {
                encoded.push(0x0B);
                encoded.extend_from_slice(&value.to_le_bytes());
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
                join: join.as_ref().clone(),
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
    let receiver = transient_topn::build_transient_topn_receiver(
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
        let first = Arc::clone(&first);
        let second = Arc::clone(&second);
        Box::pin(async move {
            let deltas = first(deltas).await?;
            second(Arc::new(deltas)).await
        })
    })
}

fn identity_delta_transform() -> Arc<DeltaTransformFn> {
    Arc::new(
        |deltas: Arc<Vec<(Vec<u8>, i64)>>| -> BoxFuture<'static, Result<Vec<(Vec<u8>, i64)>>> {
            Box::pin(async move { Ok(deltas.as_ref().clone()) })
        },
    )
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
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
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
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
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
        let evaluator = Arc::clone(&evaluator);
        Box::pin(async move {
            evaluator
                .transform_delta_arrow("source_batch_journal", delta_values)
                .await
        })
    }))
}

#[cfg(test)]
mod tests;
