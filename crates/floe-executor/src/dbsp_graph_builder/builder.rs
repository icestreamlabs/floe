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
use crate::mv::registry::MaterializedViewRegistry;
use crate::outer_stream::TransientSourceHandleStream;
use crate::task_events::{GraphTaskSender, report_graph_task_error};
use crate::vectorized_keys::{VectorizedEncodedKeyExtractor, VectorizedKeyedTimeBatch};

use super::compile::{
    PrekeyedIncrementalAggregateBatchEvaluator, build_count_aggregate_slot_kinds,
    build_count_batch_row_evaluator, build_incremental_aggregate_batch_row_evaluator,
    build_incremental_aggregate_slot_kinds, build_prekeyed_incremental_aggregate_batch_evaluator,
};
use super::materialize::{
    DeltaTransformFn, TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY, TransientMaterializeBatch,
    TransientMaterializeReceiver,
};
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
                && let Some(upstream) = inputs
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
                        &inputs.task_events,
                    )?;
                    let mut right_transient_input = try_build_transient_join_input_optimization(
                        self.graph_id(),
                        inputs.plan,
                        right_idx,
                        inputs.outer_transient_streams,
                        None,
                        &inputs.cancel,
                        &inputs.task_events,
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
                    let (tx, rx) = tokio::sync::mpsc::channel::<TransientMaterializeBatch>(
                        TRANSIENT_MATERIALIZE_CHANNEL_CAPACITY,
                    );
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

    pub(super) fn graph_id(&self) -> &str {
        &self.ns.graph_id
    }

    pub(super) fn operator_state_namespace(&self, node_idx: usize, side: &str) -> String {
        crate::namespaces::operator_state(self.graph_id(), node_idx, side)
            .unwrap_or_else(|_| format!("op_{}_{}_{}", self.graph_id(), node_idx, side))
    }
}

pub use source_requirements::{
    PlanSourceRequirements, plan_source_requirements, source_batch_journal_root_source_name,
    source_batch_journal_root_sources, source_batch_journal_root_sources_with_config,
    transient_source_root_requirements,
};
pub use types::{BuildInputs, BuildOutputs};

mod compile_node;
mod row_helpers;
mod source_requirements;
mod transient_aggregate_codec;
mod transient_aggregate_receiver;
mod transient_join_pipeline;
mod transient_join_receiver;
mod transient_precompute;
mod transient_receivers;
mod transient_roots;
mod transient_segment;
mod transient_state;
mod transient_topn;
mod transient_window_receiver;
mod types;

use row_helpers::*;
use transient_aggregate_codec::*;
use transient_aggregate_receiver::*;
use transient_join_pipeline::*;
use transient_precompute::*;
use transient_receivers::*;
use transient_roots::*;
use transient_segment::try_build_transient_segment_optimization;
use transient_state::PersistentTransientInputState;
use transient_window_receiver::*;
use types::*;

#[cfg(test)]
mod tests;
