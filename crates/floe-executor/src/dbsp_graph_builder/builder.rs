use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use anyhow::{Context, Result, anyhow, bail};
use async_recursion::async_recursion;
use dbsp::circuit::plan::DbspProjectExpr;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::storage::gc::{GcPolicy, SweepStats};
use dbsp::stream::DeltaHandleStream;
use dbsp::{
    CircuitNode, CircuitPlan, CompactionSchedulerConfig, DbspNodeKind, RowSchema, StreamRetention,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
use crate::dbsp_graph_builder::eval::{eval_expression, eval_scalar_expression};
use crate::dbsp_plan::{
    DbspProjectNode, DbspSelectNode, DbspSourceNode, ValidatedPlan, validate_dbsp_plan,
};
use crate::delta_consolidation::ConsolidationMode;
use crate::encoding::{decode_projected_row_key, encode_projected_row_key};
use crate::materialized_view::MaterializedViewRegistry;
use crate::outer_stream::TransientSourceHandleStream;
use crate::task_events::GraphTaskSender;

use super::materialize::{DeltaTransformFn, TransientMaterializeBatch};
use super::persistence_policy::{PersistencePolicy, TransientSegmentSpec, TransientSegmentStep};
use super::vectorized_filter_project::{
    VectorizedFilterProjectEvaluator, required_encoded_input_columns,
};

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

        if !matches!(root_node.kind, DbspNodeKind::Sink(_)) && inputs.enable_source_batch_journal {
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
        }

        let mut root_materialized = false;
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
                    let left_transient_input = try_build_transient_join_input_optimization(
                        self.graph_id(),
                        inputs.plan,
                        left_idx,
                        inputs.outer_transient_streams,
                        &inputs.cancel,
                    )?;
                    let right_transient_input = try_build_transient_join_input_optimization(
                        self.graph_id(),
                        inputs.plan,
                        right_idx,
                        inputs.outer_transient_streams,
                        &inputs.cancel,
                    )?;
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
                        "using transient join-to-mv root materialization"
                    );
                    self.materialize_view_from_transient_overlay_receiver(
                        inputs.view_name,
                        Arc::clone(&root_node.output_schema),
                        rx,
                        Arc::clone(&transient_opt.transform),
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
                        tx,
                        &inputs.task_events,
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
                let stream = self
                    .materialize_view(
                        inputs.view_name,
                        Arc::clone(&root_node.output_schema),
                        upstream,
                        Some(transient_opt.transform),
                        &inputs.cancel,
                        &inputs.task_events,
                        &inputs.mv_registry,
                        &mut mv_latest,
                        inputs.mv_retention,
                        self.output_consolidation_mode,
                    )
                    .await?;
                built.insert(inputs.plan.root, stream.clone());
                root_materialized = true;
                stream
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

        if !root_materialized && !mv_latest.contains_key(inputs.view_name) {
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
                self.compile_join(join, left, right, cancel, task_events)
                    .await?
            }
            DbspNodeKind::Aggregate(aggregate) => {
                let input_idx = first_input(node, "aggregate")?;
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
                self.compile_aggregate(aggregate, upstream, task_events)
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
                self.compile_distinct(distinct, upstream, task_events)
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
    let mut evaluators = Vec::new();
    for step in segment.steps {
        match step {
            TransientSegmentStep::Passthrough => {}
            TransientSegmentStep::Select { predicate, schema } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_filter(
                    &predicate, schema,
                )?);
            }
            TransientSegmentStep::Project {
                expressions,
                schema,
            } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_map(
                    expressions.as_ref(),
                    schema,
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
        transform,
    })
}

fn apply_transient_segment_vectorized(
    graph_id: &str,
    evaluators: &[VectorizedFilterProjectEvaluator],
    mut deltas: Vec<(Vec<u8>, i64)>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    for evaluator in evaluators {
        if deltas.is_empty() {
            break;
        }
        deltas = evaluator.transform_delta(graph_id, deltas)?;
    }
    Ok(deltas)
}

struct TransientSourceRootMaterialization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    transform: Arc<DeltaTransformFn>,
}

struct TransientJoinInputOptimization {
    source_name: String,
    optimized_nodes: Vec<usize>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<dbsp::join::TransientJoinInputBatch<Vec<u8>>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSourceRequirements {
    pub source_name: String,
    pub required_columns: Vec<usize>,
}

pub fn source_batch_journal_root_source_name(plan: &CircuitPlan) -> Option<String> {
    find_transient_source_root_shape(plan, plan.root)
        .ok()
        .flatten()
        .map(|shape| shape.source_name().to_string())
}

pub fn transient_source_root_requirements(
    plan: &CircuitPlan,
) -> Result<Option<TransientSourceRootRequirements>> {
    let Some(shape) = find_transient_source_root_shape(plan, plan.root)? else {
        return Ok(None);
    };
    let source_name = shape.source_name().to_string();
    let required_columns = match &shape {
        TransientSourceRootShape::Source { source, .. }
        | TransientSourceRootShape::Select { source, .. } => {
            (0..source.output_schema().len()).collect()
        }
        TransientSourceRootShape::Project { project, .. } => required_encoded_input_columns(
            None,
            Some(project.expressions()),
            project.input_schema(),
        )?,
        TransientSourceRootShape::FilterMap {
            select, project, ..
        } => required_encoded_input_columns(
            Some(select.predicate()),
            Some(project.expressions()),
            project.input_schema(),
        )?,
    };
    Ok(Some(TransientSourceRootRequirements {
        source_name,
        required_columns,
    }))
}

pub fn plan_source_requirements(plan: &CircuitPlan) -> Result<Option<Vec<PlanSourceRequirements>>> {
    let Some(root) = plan.node(plan.root) else {
        return Ok(Some(Vec::new()));
    };
    let mut required_columns_by_node: HashMap<usize, BTreeSet<usize>> = HashMap::new();
    required_columns_by_node.insert(root.id, (0..root.output_schema.len()).collect());
    let mut pending = VecDeque::from([root.id]);
    let mut required_columns_by_source: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();

    while let Some(node_id) = pending.pop_front() {
        let Some(node) = plan.node(node_id) else {
            bail!("plan source requirement analysis could not find node {node_id}");
        };
        let Some(required_columns) = required_columns_by_node.get(&node_id).cloned() else {
            continue;
        };

        match &node.kind {
            DbspNodeKind::Source(source) => {
                required_columns_by_source
                    .entry(source.table.name.to_string())
                    .or_default()
                    .extend(required_columns);
            }
            DbspNodeKind::Select(select) => {
                let input_idx = first_input(node, "select")?;
                let mut input_columns = required_columns;
                add_required_expression_columns(
                    select.predicate().expression(),
                    select.output_schema().as_ref(),
                    &mut input_columns,
                )?;
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::Project(project) => {
                let input_idx = first_input(node, "project")?;
                let mut input_columns = BTreeSet::new();
                for column_idx in required_columns {
                    let expr = project.expressions().get(column_idx).ok_or_else(|| {
                        anyhow!(
                            "required output column {column_idx} out of bounds for project node"
                        )
                    })?;
                    add_required_expression_columns(
                        expr.expression(),
                        project.input_schema().as_ref(),
                        &mut input_columns,
                    )?;
                }
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::Join(join) => {
                if node.inputs.len() != 2 {
                    bail!(
                        "join source requirement analysis expected 2 inputs, found {}",
                        node.inputs.len()
                    );
                }
                let left_idx = node.inputs[0];
                let right_idx = node.inputs[1];
                let mut left_columns = BTreeSet::new();
                let mut right_columns = BTreeSet::new();
                split_join_required_columns(
                    &required_columns,
                    join.left_schema.len(),
                    &mut left_columns,
                    &mut right_columns,
                )?;
                for key in &join.keys {
                    add_required_expression_columns(
                        key.left_expression(),
                        join.left_schema.as_ref(),
                        &mut left_columns,
                    )?;
                    add_required_expression_columns(
                        key.right_expression(),
                        join.right_schema.as_ref(),
                        &mut right_columns,
                    )?;
                }
                if let Some(residual) = &join.residual {
                    let mut residual_columns = BTreeSet::new();
                    add_required_expression_columns(
                        residual,
                        join.output_schema.as_ref(),
                        &mut residual_columns,
                    )?;
                    split_join_required_columns(
                        &residual_columns,
                        join.left_schema.len(),
                        &mut left_columns,
                        &mut right_columns,
                    )?;
                }
                if extend_required_columns(&mut required_columns_by_node, left_idx, left_columns) {
                    pending.push_back(left_idx);
                }
                if extend_required_columns(&mut required_columns_by_node, right_idx, right_columns)
                {
                    pending.push_back(right_idx);
                }
            }
            DbspNodeKind::Passthrough | DbspNodeKind::Sink(_) => {
                let operator = match &node.kind {
                    DbspNodeKind::Passthrough => "passthrough",
                    DbspNodeKind::Sink(_) => "sink",
                    _ => unreachable!(),
                };
                let input_idx = first_input(node, operator)?;
                if extend_required_columns(
                    &mut required_columns_by_node,
                    input_idx,
                    required_columns,
                ) {
                    pending.push_back(input_idx);
                }
            }
            _ => return Ok(None),
        }
    }

    Ok(Some(
        required_columns_by_source
            .into_iter()
            .map(|(source_name, required_columns)| PlanSourceRequirements {
                source_name,
                required_columns: required_columns.into_iter().collect(),
            })
            .collect(),
    ))
}

fn extend_required_columns(
    required_columns_by_node: &mut HashMap<usize, BTreeSet<usize>>,
    node_idx: usize,
    columns: BTreeSet<usize>,
) -> bool {
    let entry = required_columns_by_node.entry(node_idx).or_default();
    let previous_len = entry.len();
    entry.extend(columns);
    entry.len() != previous_len
}

fn add_required_expression_columns(
    expression: &dbsp::DbspExpression,
    input_schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<()> {
    for column in expression.expr().column_refs() {
        let column_idx = input_schema
            .field_index(column.name.as_str())
            .ok_or_else(|| anyhow!("column '{}' not found in input schema", column.name))?;
        columns.insert(column_idx);
    }
    Ok(())
}

fn split_join_required_columns(
    columns: &BTreeSet<usize>,
    left_width: usize,
    left_columns: &mut BTreeSet<usize>,
    right_columns: &mut BTreeSet<usize>,
) -> Result<()> {
    for column_idx in columns {
        if *column_idx < left_width {
            left_columns.insert(*column_idx);
            continue;
        }
        let right_idx = column_idx
            .checked_sub(left_width)
            .ok_or_else(|| anyhow!("join column index underflow for {column_idx}"))?;
        right_columns.insert(right_idx);
    }
    Ok(())
}

fn try_build_transient_join_input_optimization(
    graph_id: &str,
    plan: &CircuitPlan,
    input_idx: usize,
    outer_transient_streams: &HashMap<String, TransientSourceHandleStream>,
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
    let cancel = cancel.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_batch = upstream_rx.recv() => {
                    let Some(batch) = maybe_batch else {
                        break;
                    };
                    let transformed = match transform(batch.deltas.as_ref().clone()) {
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
                    if tx.send(dbsp::join::TransientJoinInputBatch {
                        ts: join_ts,
                        deltas: Arc::new(transformed),
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

fn try_build_transient_source_root_materialization(
    plan: &CircuitPlan,
    root_idx: usize,
) -> Result<Option<TransientSourceRootMaterialization>> {
    let Some(shape) = find_transient_source_root_shape(plan, root_idx)? else {
        return Ok(None);
    };
    let source_name = shape.source_name().to_string();
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
    let transform = match shape {
        TransientSourceRootShape::Source { .. } => {
            Ok(Arc::new(|deltas| Ok(deltas)) as Arc<DeltaTransformFn>)
        }
        TransientSourceRootShape::Select { select, .. } => build_filter_transform(&select),
        TransientSourceRootShape::Project { project, .. } => build_map_transform(&project),
        TransientSourceRootShape::FilterMap {
            select, project, ..
        } => build_filter_map_transform(&select, &project),
    }?;
    Ok(Some(TransientSourceRootMaterialization {
        source_name,
        optimized_nodes,
        transform,
    }))
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
    if let Ok(evaluator) =
        VectorizedFilterProjectEvaluator::for_filter(&predicate, Arc::clone(&schema))
    {
        let evaluator = Arc::new(evaluator);
        return Ok(Arc::new(move |delta_values| {
            evaluator.transform_delta("source_batch_journal", delta_values)
        }));
    }
    Ok(Arc::new(move |delta_values| {
        let mut staged = Vec::with_capacity(delta_values.len());
        for (encoded, diff) in delta_values {
            if diff == 0 {
                continue;
            }
            let row = match decode_projected_row_key(&encoded) {
                Ok(row) => row,
                Err(_) => continue,
            };
            if matches!(eval_predicate(&predicate, &row, schema.as_ref()), Ok(true)) {
                staged.push((encoded, diff));
            }
        }
        Ok(staged)
    }))
}

fn build_map_transform(node: &DbspProjectNode) -> Result<Arc<DeltaTransformFn>> {
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(node.expressions().to_vec());
    let schema = Arc::clone(node.input_schema());
    if let Ok(evaluator) =
        VectorizedFilterProjectEvaluator::for_map(expressions.as_ref(), Arc::clone(&schema))
    {
        let evaluator = Arc::new(evaluator);
        return Ok(Arc::new(move |delta_values| {
            evaluator.transform_delta("source_batch_journal", delta_values)
        }));
    }
    Ok(Arc::new(move |delta_values| {
        let mut staged = Vec::with_capacity(delta_values.len());
        for (encoded, diff) in delta_values {
            if diff == 0 {
                continue;
            }
            let row = match decode_projected_row_key(&encoded) {
                Ok(row) => row,
                Err(_) => continue,
            };
            let projected = match eval_projection(expressions.as_ref(), &row, schema.as_ref()) {
                Ok(projected) => projected,
                Err(_) => continue,
            };
            let encoded = match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(_) => continue,
            };
            staged.push((encoded, diff));
        }
        Ok(staged)
    }))
}

fn build_filter_map_transform(
    select: &DbspSelectNode,
    project: &DbspProjectNode,
) -> Result<Arc<DeltaTransformFn>> {
    let predicate = select.predicate().clone();
    let filter_schema = Arc::clone(select.output_schema());
    let expressions: Arc<Vec<DbspProjectExpr>> = Arc::new(project.expressions().to_vec());
    let project_schema = Arc::clone(project.input_schema());
    if let Ok(evaluator) = VectorizedFilterProjectEvaluator::for_filter_map(
        &predicate,
        expressions.as_ref(),
        Arc::clone(&project_schema),
    ) {
        let evaluator = Arc::new(evaluator);
        return Ok(Arc::new(move |delta_values| {
            evaluator.transform_delta("source_batch_journal", delta_values)
        }));
    }
    Ok(Arc::new(move |delta_values| {
        let mut staged = Vec::with_capacity(delta_values.len());
        for (encoded, diff) in delta_values {
            if diff == 0 {
                continue;
            }
            let row = match decode_projected_row_key(&encoded) {
                Ok(row) => row,
                Err(_) => continue,
            };
            match eval_predicate(&predicate, &row, filter_schema.as_ref()) {
                Ok(true) => {}
                Ok(false) | Err(_) => continue,
            }
            let projected =
                match eval_projection(expressions.as_ref(), &row, project_schema.as_ref()) {
                    Ok(projected) => projected,
                    Err(_) => continue,
                };
            let encoded = match encode_projected_row_key(&projected) {
                Ok(encoded) => encoded,
                Err(_) => continue,
            };
            staged.push((encoded, diff));
        }
        Ok(staged)
    }))
}

fn eval_predicate(
    predicate: &dbsp::DbspPredicate,
    row: &[datafusion::scalar::ScalarValue],
    schema: &RowSchema,
) -> Result<bool> {
    eval_expression(predicate.expression(), row, schema)
}

fn eval_projection(
    expressions: &[DbspProjectExpr],
    row: &[datafusion::scalar::ScalarValue],
    schema: &RowSchema,
) -> Result<Vec<datafusion::scalar::ScalarValue>> {
    expressions
        .iter()
        .map(|expr| eval_scalar_expression(expr.expression(), row, schema))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    use std::time::Duration;

    use datafusion::common::Column;
    use datafusion::logical_expr::{JoinType, LogicalPlan, col, lit, table_scan};
    use dbsp::DbspJoin;
    use dbsp::join::TransientJoinInputBatch;
    use dbsp::storage::{KeyValueTable, SlateTable};
    use dbsp::stream::StreamCursor;
    use dbsp::stream::util::materialize_zset_handle;
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition, SourceEvent};
    use object_store::memory::InMemory;
    use serde_json::{Value, json};
    use slatedb::Db;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::GraphTaskError;
    use crate::dbsp_bridge::DbspBridge;
    use crate::dbsp_plan::{
        DbspPlanBuilder, nexmark_auction_table, nexmark_bid_table, nexmark_config,
        validate_dbsp_plan,
    };
    use crate::materialized_view::MaterializedViewRegistry;
    use crate::outer_stream::OuterStreamRegistry;
    use crate::source_decoder::SourceRowDecoder;

    #[test]
    fn benchmark_join_shape_still_matches_transient_join_root() {
        let logical = benchmark_join_logical_plan();
        let planner = DbspPlanBuilder::new(nexmark_config());
        let plan = planner.build(&logical).expect("circuit plan");
        let persistence_policy = PersistencePolicy::for_plan(&plan);
        let transient_opt = try_build_transient_segment_optimization(
            &plan,
            plan.root,
            &HashMap::new(),
            "benchmark_result",
            true,
            &persistence_policy,
        )
        .expect("transient optimization result");

        assert!(
            transient_opt.is_some(),
            "expected transient optimization for benchmark query plan: {plan:#?}"
        );
        let transient_opt = transient_opt.expect("transient opt");
        let join_node = plan
            .node(transient_opt.durable_input_idx)
            .expect("durable input node");
        assert!(
            matches!(join_node.kind, DbspNodeKind::Join(_)),
            "expected durable input to be a join node: {plan:#?}"
        );
        let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");
        assert!(
            try_build_transient_source_root_materialization(&plan, left_idx)
                .expect("left transient input shape")
                .is_some(),
            "expected left benchmark join input to be transient-eligible: {plan:#?}"
        );
        assert!(
            try_build_transient_source_root_materialization(&plan, right_idx)
                .expect("right transient input shape")
                .is_some(),
            "expected right benchmark join input to be transient-eligible: {plan:#?}"
        );
    }

    #[tokio::test]
    async fn benchmark_join_child_transforms_match_pruned_source_handle_outputs() {
        let db = test_db("benchmark-join-child-transform-equivalence").await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
        let view_name = "benchmark_result";
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

        let logical = benchmark_join_logical_plan();
        let planner = DbspPlanBuilder::new(nexmark_config());
        let plan = planner.build(&logical).expect("circuit plan");
        let persistence_policy = PersistencePolicy::for_plan(&plan);
        let root_transient = try_build_transient_segment_optimization(
            &plan,
            plan.root,
            &HashMap::new(),
            view_name,
            true,
            &persistence_policy,
        )
        .expect("root transient opt")
        .expect("root transient opt");
        let join_node = plan
            .node(root_transient.durable_input_idx)
            .expect("join node");
        let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

        let left_transient = try_build_transient_segment_optimization(
            &plan,
            left_idx,
            &HashMap::new(),
            "left_child",
            false,
            &persistence_policy,
        )
        .expect("left transient opt")
        .expect("left transient opt");
        let right_transient = try_build_transient_segment_optimization(
            &plan,
            right_idx,
            &HashMap::new(),
            "right_child",
            false,
            &persistence_policy,
        )
        .expect("right transient opt")
        .expect("right transient opt");

        let requirements = plan_source_requirements(&plan)
            .expect("source requirements")
            .expect("source requirements");
        let bid_definition = nexmark_bid_source_definition();
        let auction_definition = nexmark_auction_source_definition();
        let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
        let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

        let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            bid_definition,
            Some(Arc::clone(&bid_mask)),
        );
        let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            auction_definition,
            Some(Arc::clone(&auction_mask)),
        );

        let available_sources = ["nexmark_bid", "nexmark_auction"]
            .into_iter()
            .map(|name| name.to_string())
            .collect::<BTreeSet<_>>();
        let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
            .expect("validate plan")
            .required_sources;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
                .await
                .expect("outer streams");

        let handle_streams = required_sources
            .iter()
            .filter_map(|source| {
                registry
                    .delta_handle_stream(source)
                    .map(|stream| (source.clone(), stream))
            })
            .collect::<HashMap<_, _>>();

        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        builder.watermark = Arc::new(AtomicI64::new(-1));
        builder.ns.set_graph_id(view_name);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut mv_latest = HashMap::new();
        let mut built = HashMap::new();
        let cancel = CancellationToken::new();
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        let left_stream = builder
            .compile_node(
                &plan,
                left_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile left child");
        let right_stream = builder
            .compile_node(
                &plan,
                right_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile right child");

        let mut left_cursor = StreamCursor::new(left_stream.stream());
        let mut right_cursor = StreamCursor::new(right_stream.stream());
        let _ = left_cursor.snapshot().await.expect("left initial snapshot");
        let _ = right_cursor
            .snapshot()
            .await
            .expect("right initial snapshot");

        let auction_batch = vec![
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(1, 100, 10),
                    "nexmark_auction",
                ),
                1,
            ),
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(2, 200, 5),
                    "nexmark_auction",
                ),
                1,
            ),
        ];
        {
            let writer = registry
                .writer_mut("nexmark_auction")
                .expect("auction writer");
            for (encoded, diff) in &auction_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded auction");
            }
        }
        registry
            .tick_all_with_version(1)
            .await
            .expect("tick auction batch");
        assert_tick_matches_transform(
            &table,
            &mut left_cursor,
            Vec::new(),
            &left_transient.transform,
            "left tick 1",
        )
        .await;
        assert_tick_matches_transform(
            &table,
            &mut right_cursor,
            auction_batch,
            &right_transient.transform,
            "right tick 1",
        )
        .await;

        for tick in 0..64usize {
            let bid_batch = vec![
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
            ];
            {
                let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
                for (encoded, diff) in &bid_batch {
                    writer
                        .append_encoded(encoded.clone(), *diff)
                        .expect("append encoded bid");
                }
            }
            registry
                .tick_all_with_version(i64::try_from(tick + 2).expect("tick version"))
                .await
                .expect("tick bid batch");
            assert_tick_matches_transform(
                &table,
                &mut left_cursor,
                bid_batch,
                &left_transient.transform,
                "left bid tick",
            )
            .await;
            assert_tick_matches_transform(
                &table,
                &mut right_cursor,
                Vec::new(),
                &right_transient.transform,
                "right bid tick",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn benchmark_large_bid_batch_transform_matches_pruned_source_handle_output() {
        let db = test_db("benchmark-large-bid-transform-equivalence").await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
        let view_name = "benchmark_result";
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

        let logical = benchmark_join_logical_plan();
        let planner = DbspPlanBuilder::new(nexmark_config());
        let plan = planner.build(&logical).expect("circuit plan");
        let persistence_policy = PersistencePolicy::for_plan(&plan);
        let root_transient = try_build_transient_segment_optimization(
            &plan,
            plan.root,
            &HashMap::new(),
            view_name,
            true,
            &persistence_policy,
        )
        .expect("root transient opt")
        .expect("root transient opt");
        let join_node = plan
            .node(root_transient.durable_input_idx)
            .expect("join node");
        let (left_idx, _right_idx) = join_inputs(join_node).expect("join inputs");

        let left_transient = try_build_transient_segment_optimization(
            &plan,
            left_idx,
            &HashMap::new(),
            "left_child",
            false,
            &persistence_policy,
        )
        .expect("left transient opt")
        .expect("left transient opt");

        let requirements = plan_source_requirements(&plan)
            .expect("source requirements")
            .expect("source requirements");
        let bid_definition = nexmark_bid_source_definition();
        let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
        let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            bid_definition,
            Some(Arc::clone(&bid_mask)),
        );

        let available_sources = ["nexmark_bid", "nexmark_auction"]
            .into_iter()
            .map(|name| name.to_string())
            .collect::<BTreeSet<_>>();
        let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
            .expect("validate plan")
            .required_sources;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
                .await
                .expect("outer streams");

        let handle_streams = required_sources
            .iter()
            .filter_map(|source| {
                registry
                    .delta_handle_stream(source)
                    .map(|stream| (source.clone(), stream))
            })
            .collect::<HashMap<_, _>>();

        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        builder.watermark = Arc::new(AtomicI64::new(-1));
        builder.ns.set_graph_id(view_name);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut mv_latest = HashMap::new();
        let mut built = HashMap::new();
        let cancel = CancellationToken::new();
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        let left_stream = builder
            .compile_node(
                &plan,
                left_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile left child");
        let mut left_cursor = StreamCursor::new(left_stream.stream());
        let _ = left_cursor.snapshot().await.expect("left initial snapshot");

        let full_batch = (0..16_384usize)
            .map(|offset| {
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(
                            i64::try_from((offset % 10_000) + 1).expect("auction id"),
                            1_000_000 + i64::try_from(offset).expect("bidder"),
                            10_000 + i64::try_from(offset).expect("price"),
                        ),
                        "nexmark_bid",
                    ),
                    1,
                )
            })
            .collect::<Vec<_>>();
        {
            let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            for (encoded, diff) in &full_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded bid full batch");
            }
        }
        registry
            .tick_all_with_version(1)
            .await
            .expect("tick full bid batch");
        assert_tick_matches_transform(
            &table,
            &mut left_cursor,
            full_batch,
            &left_transient.transform,
            "left full 16k batch",
        )
        .await;

        let partial_batch = (0..576usize)
            .map(|offset| {
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(
                            i64::try_from((offset % 10_000) + 1).expect("auction id"),
                            2_000_000 + i64::try_from(offset).expect("bidder"),
                            20_000 + i64::try_from(offset).expect("price"),
                        ),
                        "nexmark_bid",
                    ),
                    1,
                )
            })
            .collect::<Vec<_>>();
        {
            let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
            for (encoded, diff) in &partial_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded bid partial batch");
            }
        }
        registry
            .tick_all_with_version(2)
            .await
            .expect("tick partial bid batch");
        assert_tick_matches_transform(
            &table,
            &mut left_cursor,
            partial_batch,
            &left_transient.transform,
            "left partial 576 batch",
        )
        .await;
    }

    #[tokio::test]
    async fn benchmark_transient_join_inputs_match_canonical_join_output() {
        let db = test_db("benchmark-join-transient-input-equivalence").await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
        let view_name = "benchmark_result";
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

        let logical = benchmark_join_logical_plan();
        let planner = DbspPlanBuilder::new(nexmark_config());
        let plan = planner.build(&logical).expect("circuit plan");
        let persistence_policy = PersistencePolicy::for_plan(&plan);
        let root_transient = try_build_transient_segment_optimization(
            &plan,
            plan.root,
            &HashMap::new(),
            view_name,
            true,
            &persistence_policy,
        )
        .expect("root transient opt")
        .expect("root transient opt");
        let join_node = plan
            .node(root_transient.durable_input_idx)
            .expect("join node");
        let join = match &join_node.kind {
            DbspNodeKind::Join(join) => join.clone(),
            other => panic!("expected join node, got {other:?}"),
        };
        let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

        let left_transient = try_build_transient_segment_optimization(
            &plan,
            left_idx,
            &HashMap::new(),
            "left_child",
            false,
            &persistence_policy,
        )
        .expect("left transient opt")
        .expect("left transient opt");
        let right_transient = try_build_transient_segment_optimization(
            &plan,
            right_idx,
            &HashMap::new(),
            "right_child",
            false,
            &persistence_policy,
        )
        .expect("right transient opt")
        .expect("right transient opt");

        let requirements = plan_source_requirements(&plan)
            .expect("source requirements")
            .expect("source requirements");
        let bid_definition = nexmark_bid_source_definition();
        let auction_definition = nexmark_auction_source_definition();
        let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
        let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

        let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            bid_definition,
            Some(Arc::clone(&bid_mask)),
        );
        let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            auction_definition,
            Some(Arc::clone(&auction_mask)),
        );

        let available_sources = ["nexmark_bid", "nexmark_auction"]
            .into_iter()
            .map(|name| name.to_string())
            .collect::<BTreeSet<_>>();
        let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
            .expect("validate plan")
            .required_sources;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
                .await
                .expect("outer streams");

        let handle_streams = required_sources
            .iter()
            .filter_map(|source| {
                registry
                    .delta_handle_stream(source)
                    .map(|stream| (source.clone(), stream))
            })
            .collect::<HashMap<_, _>>();

        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        builder.watermark = Arc::new(AtomicI64::new(-1));
        builder.ns.set_graph_id(view_name);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut mv_latest = HashMap::new();
        let mut built = HashMap::new();
        let cancel = CancellationToken::new();
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        let left_stream = builder
            .compile_node(
                &plan,
                left_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile left child");
        let right_stream = builder
            .compile_node(
                &plan,
                right_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile right child");

        let join_keys = Arc::new(join.keys.clone());
        let left_schema = Arc::clone(&join.left_schema);
        let right_schema = Arc::clone(&join.right_schema);
        let output_schema = Arc::clone(&join.output_schema);
        let residual = join.residual.clone();

        let left_key = {
            let join_keys = Arc::clone(&join_keys);
            let left_schema = Arc::clone(&left_schema);
            move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                let left_row = decode_projected_row_key(left_bytes).ok()?;
                let mut key_columns = Vec::with_capacity(join_keys.len());
                for key in join_keys.iter() {
                    let value = eval_scalar_expression(
                        key.left_expression(),
                        &left_row,
                        left_schema.as_ref(),
                    )
                    .ok()?;
                    if value.is_null() {
                        return None;
                    }
                    key_columns.push(value);
                }
                encode_projected_row_key(&key_columns).ok()
            }
        };
        let right_key = {
            let join_keys = Arc::clone(&join_keys);
            let right_schema = Arc::clone(&right_schema);
            move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                let right_row = decode_projected_row_key(right_bytes).ok()?;
                let mut key_columns = Vec::with_capacity(join_keys.len());
                for key in join_keys.iter() {
                    let value = eval_scalar_expression(
                        key.right_expression(),
                        &right_row,
                        right_schema.as_ref(),
                    )
                    .ok()?;
                    if value.is_null() {
                        return None;
                    }
                    key_columns.push(value);
                }
                encode_projected_row_key(&key_columns).ok()
            }
        };
        let predicate = {
            let residual = residual.clone();
            let output_schema = Arc::clone(&output_schema);
            move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
                let Some(expr) = residual.as_ref() else {
                    return true;
                };
                let left_row = match decode_projected_row_key(left_bytes) {
                    Ok(row) => row,
                    Err(_) => return false,
                };
                let right_row = match decode_projected_row_key(right_bytes) {
                    Ok(row) => row,
                    Err(_) => return false,
                };
                let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                combined.extend(left_row);
                combined.extend(right_row);
                eval_expression(expr, &combined, output_schema.as_ref()).unwrap_or(false)
            }
        };
        let projector = |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
            crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
        };

        let canonical_join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_stream,
            &right_stream,
            left_key.clone(),
            right_key.clone(),
            predicate.clone(),
            projector,
            None,
        )
        .await
        .expect("canonical join");
        let mut canonical_cursor = StreamCursor::new(canonical_join.stream().stream());
        let _ = canonical_cursor
            .snapshot()
            .await
            .expect("initial canonical join snapshot");

        let (observer_tx, mut observer_rx) =
            mpsc::unbounded_channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>();
        let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
            let _ = observer_tx.send((version, deltas));
        });
        let (left_transient_tx, left_transient_rx) =
            mpsc::unbounded_channel::<TransientJoinInputBatch<Vec<u8>>>();
        let (right_transient_tx, right_transient_rx) =
            mpsc::unbounded_channel::<TransientJoinInputBatch<Vec<u8>>>();
        DbspJoin::spawn_transient_with_inputs::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_stream,
            &right_stream,
            Some(left_transient_rx),
            Some(right_transient_rx),
            left_key,
            right_key,
            predicate,
            |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
                crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
            },
            observer,
            None,
        )
        .await
        .expect("transient join with inputs");

        let auction_batch = vec![
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(1, 100, 10),
                    "nexmark_auction",
                ),
                1,
            ),
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(2, 200, 5),
                    "nexmark_auction",
                ),
                1,
            ),
        ];
        let transformed_right_tick1 =
            (right_transient.transform)(auction_batch.clone()).expect("transform auction batch");
        right_transient_tx
            .send(TransientJoinInputBatch {
                ts: 1,
                deltas: Arc::new(transformed_right_tick1),
            })
            .expect("send auction transient batch");
        {
            let writer = registry
                .writer_mut("nexmark_auction")
                .expect("auction writer");
            for (encoded, diff) in &auction_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded auction");
            }
        }
        registry
            .tick_all_with_version(1)
            .await
            .expect("tick auction batch");
        assert!(
            timeout(Duration::from_millis(100), canonical_cursor.next())
                .await
                .is_err(),
            "auction build tick should not emit canonical join output"
        );
        assert!(
            timeout(Duration::from_millis(100), observer_rx.recv())
                .await
                .is_err(),
            "auction build tick should not emit transient join output"
        );

        let mut cache = HashMap::new();
        for tick in 0..64usize {
            let ts = i64::try_from(tick + 2).expect("tick version");
            let bid_batch = vec![
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
            ];
            let transformed_left =
                (left_transient.transform)(bid_batch.clone()).expect("transform bid batch");
            if tick != 16 {
                left_transient_tx
                    .send(TransientJoinInputBatch {
                        ts,
                        deltas: Arc::new(transformed_left),
                    })
                    .expect("send bid transient batch");
            }
            right_transient_tx
                .send(TransientJoinInputBatch {
                    ts,
                    deltas: Arc::new(Vec::new()),
                })
                .expect("send empty right transient batch");
            {
                let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
                for (encoded, diff) in &bid_batch {
                    writer
                        .append_encoded(encoded.clone(), *diff)
                        .expect("append encoded bid");
                }
            }
            registry
                .tick_all_with_version(ts)
                .await
                .expect("tick bid batch");

            let (_, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
                .await
                .expect("wait canonical join output")
                .expect("canonical join output");
            let actual = materialize_zset_handle::<Vec<u8>>(
                Arc::clone(&table),
                &mut cache,
                &canonical_handle,
            )
            .await
            .expect("materialize canonical join delta");

            let (version, transient_batch) = timeout(Duration::from_secs(1), observer_rx.recv())
                .await
                .expect("wait transient join output")
                .expect("transient join output");
            assert_eq!(
                version,
                i64::try_from(tick + 1).expect("transient output version"),
                "unexpected transient join output version at bid tick {tick}"
            );
            let expected = consolidate_encoded_deltas(transient_batch.as_ref().clone());
            assert_eq!(actual, expected, "join output mismatch at bid tick {tick}");
        }
    }

    #[tokio::test]
    async fn benchmark_transient_source_task_join_inputs_match_canonical_join_output() {
        let db = test_db("benchmark-join-source-task-input-equivalence").await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));
        let view_name = "benchmark_result";
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");

        let logical = benchmark_join_logical_plan();
        let planner = DbspPlanBuilder::new(nexmark_config());
        let plan = planner.build(&logical).expect("circuit plan");
        let persistence_policy = PersistencePolicy::for_plan(&plan);
        let root_transient = try_build_transient_segment_optimization(
            &plan,
            plan.root,
            &HashMap::new(),
            view_name,
            true,
            &persistence_policy,
        )
        .expect("root transient opt")
        .expect("root transient opt");
        let join_node = plan
            .node(root_transient.durable_input_idx)
            .expect("join node");
        let join = match &join_node.kind {
            DbspNodeKind::Join(join) => join.clone(),
            other => panic!("expected join node, got {other:?}"),
        };
        let (left_idx, right_idx) = join_inputs(join_node).expect("join inputs");

        let requirements = plan_source_requirements(&plan)
            .expect("source requirements")
            .expect("source requirements");
        let bid_definition = nexmark_bid_source_definition();
        let auction_definition = nexmark_auction_source_definition();
        let bid_mask = required_mask(&requirements, &bid_definition, "nexmark_bid");
        let auction_mask = required_mask(&requirements, &auction_definition, "nexmark_auction");

        let bid_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            bid_definition,
            Some(Arc::clone(&bid_mask)),
        );
        let auction_decoder = SourceRowDecoder::new_with_encoded_required_columns(
            auction_definition,
            Some(Arc::clone(&auction_mask)),
        );

        let available_sources = ["nexmark_bid", "nexmark_auction"]
            .into_iter()
            .map(|name| name.to_string())
            .collect::<BTreeSet<_>>();
        let required_sources = validate_dbsp_plan(&plan, &available_sources, view_name)
            .expect("validate plan")
            .required_sources;
        let mut registry =
            OuterStreamRegistry::from_validated_sources(&required_sources, &mut bridge)
                .await
                .expect("outer streams");

        let handle_streams = required_sources
            .iter()
            .filter_map(|source| {
                registry
                    .delta_handle_stream(source)
                    .map(|stream| (source.clone(), stream))
            })
            .collect::<HashMap<_, _>>();
        let transient_streams = required_sources
            .iter()
            .filter_map(|source| {
                registry
                    .transient_stream(source)
                    .map(|stream| (source.clone(), stream))
            })
            .collect::<HashMap<_, _>>();

        let mut builder = DbspGraphBuilder::new(Arc::clone(&db))
            .await
            .expect("builder");
        builder.watermark = Arc::new(AtomicI64::new(-1));
        builder.ns.set_graph_id(view_name);

        let mv_registry = Arc::new(MaterializedViewRegistry::new());
        let mut mv_latest = HashMap::new();
        let mut built = HashMap::new();
        let cancel = CancellationToken::new();
        let (task_tx, _task_rx) = mpsc::unbounded_channel::<GraphTaskError>();
        let left_stream = builder
            .compile_node(
                &plan,
                left_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile left child");
        let right_stream = builder
            .compile_node(
                &plan,
                right_idx,
                &handle_streams,
                &cancel,
                &task_tx,
                &mut built,
                &mv_registry,
                &mut mv_latest,
                dbsp::StreamRetention::KeepLast { keep_last: 1 },
                &persistence_policy,
            )
            .await
            .expect("compile right child");

        let left_transient = try_build_transient_join_input_optimization(
            builder.graph_id(),
            &plan,
            left_idx,
            &transient_streams,
            &cancel,
        )
        .expect("left transient input opt")
        .expect("left transient input opt");
        let right_transient = try_build_transient_join_input_optimization(
            builder.graph_id(),
            &plan,
            right_idx,
            &transient_streams,
            &cancel,
        )
        .expect("right transient input opt")
        .expect("right transient input opt");

        let join_keys = Arc::new(join.keys.clone());
        let left_schema = Arc::clone(&join.left_schema);
        let right_schema = Arc::clone(&join.right_schema);
        let output_schema = Arc::clone(&join.output_schema);
        let residual = join.residual.clone();

        let left_key = {
            let join_keys = Arc::clone(&join_keys);
            let left_schema = Arc::clone(&left_schema);
            move |left_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                let left_row = decode_projected_row_key(left_bytes).ok()?;
                let mut key_columns = Vec::with_capacity(join_keys.len());
                for key in join_keys.iter() {
                    let value = eval_scalar_expression(
                        key.left_expression(),
                        &left_row,
                        left_schema.as_ref(),
                    )
                    .ok()?;
                    if value.is_null() {
                        return None;
                    }
                    key_columns.push(value);
                }
                encode_projected_row_key(&key_columns).ok()
            }
        };
        let right_key = {
            let join_keys = Arc::clone(&join_keys);
            let right_schema = Arc::clone(&right_schema);
            move |right_bytes: &Vec<u8>| -> Option<Vec<u8>> {
                let right_row = decode_projected_row_key(right_bytes).ok()?;
                let mut key_columns = Vec::with_capacity(join_keys.len());
                for key in join_keys.iter() {
                    let value = eval_scalar_expression(
                        key.right_expression(),
                        &right_row,
                        right_schema.as_ref(),
                    )
                    .ok()?;
                    if value.is_null() {
                        return None;
                    }
                    key_columns.push(value);
                }
                encode_projected_row_key(&key_columns).ok()
            }
        };
        let predicate = {
            let residual = residual.clone();
            let output_schema = Arc::clone(&output_schema);
            move |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> bool {
                let Some(expr) = residual.as_ref() else {
                    return true;
                };
                let left_row = match decode_projected_row_key(left_bytes) {
                    Ok(row) => row,
                    Err(_) => return false,
                };
                let right_row = match decode_projected_row_key(right_bytes) {
                    Ok(row) => row,
                    Err(_) => return false,
                };
                let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                combined.extend(left_row);
                combined.extend(right_row);
                eval_expression(expr, &combined, output_schema.as_ref()).unwrap_or(false)
            }
        };

        let canonical_join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_stream,
            &right_stream,
            left_key.clone(),
            right_key.clone(),
            predicate.clone(),
            |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
                crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
            },
            None,
        )
        .await
        .expect("canonical join");
        let mut canonical_cursor = StreamCursor::new(canonical_join.stream().stream());
        let _ = canonical_cursor
            .snapshot()
            .await
            .expect("initial canonical join snapshot");

        let (observer_tx, mut observer_rx) =
            mpsc::unbounded_channel::<(i64, Arc<Vec<(Vec<u8>, i64)>>)>();
        let observer = Arc::new(move |version: i64, deltas: Arc<Vec<(Vec<u8>, i64)>>| {
            let _ = observer_tx.send((version, deltas));
        });
        DbspJoin::spawn_transient_with_inputs::<Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, _, _, _, _>(
            &left_stream,
            &right_stream,
            Some(left_transient.receiver),
            Some(right_transient.receiver),
            left_key,
            right_key,
            predicate,
            |left_bytes: &Vec<u8>, right_bytes: &Vec<u8>| -> Vec<u8> {
                crate::encoding::concat_encoded_rows(left_bytes, right_bytes).unwrap_or_default()
            },
            observer,
            None,
        )
        .await
        .expect("transient join with source tasks");

        let auction_batch = vec![
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(1, 100, 10),
                    "nexmark_auction",
                ),
                1,
            ),
            (
                encode_event(
                    &auction_decoder,
                    auction_event_payload(2, 200, 5),
                    "nexmark_auction",
                ),
                1,
            ),
        ];
        {
            let writer = registry
                .writer_mut("nexmark_auction")
                .expect("auction writer");
            for (encoded, diff) in &auction_batch {
                writer
                    .append_encoded(encoded.clone(), *diff)
                    .expect("append encoded auction");
            }
        }
        registry
            .tick_all_with_version(1)
            .await
            .expect("tick auction batch");
        assert!(
            timeout(Duration::from_millis(100), canonical_cursor.next())
                .await
                .is_err(),
            "auction build tick should not emit canonical join output"
        );
        assert!(
            timeout(Duration::from_millis(100), observer_rx.recv())
                .await
                .is_err(),
            "auction build tick should not emit transient join output"
        );

        let mut cache = HashMap::new();
        for tick in 0..64usize {
            let ts = i64::try_from(tick + 2).expect("tick version");
            let bid_batch = vec![
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(1, 1_000 + tick as i64, 10 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
                (
                    encode_event(
                        &bid_decoder,
                        bid_event_payload(2, 2_000 + tick as i64, 20 + tick as i64),
                        "nexmark_bid",
                    ),
                    1,
                ),
            ];
            {
                let writer = registry.writer_mut("nexmark_bid").expect("bid writer");
                for (encoded, diff) in &bid_batch {
                    writer
                        .append_encoded(encoded.clone(), *diff)
                        .expect("append encoded bid");
                }
            }
            registry
                .tick_all_with_version(ts)
                .await
                .expect("tick bid batch");

            let (_, canonical_handle) = timeout(Duration::from_secs(1), canonical_cursor.next())
                .await
                .expect("wait canonical join output")
                .expect("canonical join output");
            let actual = materialize_zset_handle::<Vec<u8>>(
                Arc::clone(&table),
                &mut cache,
                &canonical_handle,
            )
            .await
            .expect("materialize canonical join delta");

            let (version, transient_batch) = timeout(Duration::from_secs(1), observer_rx.recv())
                .await
                .expect("wait transient join output")
                .expect("transient join output");
            assert_eq!(
                version,
                i64::try_from(tick + 1).expect("transient output version"),
                "unexpected transient join output version at bid tick {tick}"
            );
            let expected = consolidate_encoded_deltas(transient_batch.as_ref().clone());
            assert_eq!(actual, expected, "join output mismatch at bid tick {tick}");
        }
    }

    fn benchmark_join_logical_plan() -> LogicalPlan {
        let bid = nexmark_bid_table();
        let auction = nexmark_auction_table();
        let bid_schema = bid.schema().to_arrow_schema();
        let auction_schema = auction.schema().to_arrow_schema();
        table_scan(Some("nexmark_bid"), &bid_schema, None)
            .expect("bid scan")
            .join(
                table_scan(Some("nexmark_auction"), &auction_schema, None)
                    .expect("auction scan")
                    .build()
                    .expect("auction logical"),
                JoinType::Inner,
                (
                    vec![Column::from_name("auction")],
                    vec![Column::from_name("id")],
                ),
                None,
            )
            .expect("join")
            .filter(col("category").eq(lit(10i64)))
            .expect("filter")
            .project(vec![
                col("auction"),
                col("bidder"),
                col("price").alias("projected_price"),
                col("seller"),
            ])
            .expect("project")
            .build()
            .expect("logical plan")
    }

    async fn assert_tick_matches_transform(
        table: &Arc<dyn KeyValueTable>,
        cursor: &mut StreamCursor<dbsp::handles::ZSetHandle>,
        source_batch: Vec<(Vec<u8>, i64)>,
        transform: &Arc<DeltaTransformFn>,
        label: &str,
    ) {
        let (_, handle) = cursor.next().await.expect("next child handle");
        let mut cache = HashMap::new();
        let actual = materialize_zset_handle::<Vec<u8>>(Arc::clone(table), &mut cache, &handle)
            .await
            .expect("materialize child handle");
        let expected = consolidate_encoded_deltas(transform(source_batch).expect("transform"));
        assert_eq!(actual, expected, "{label}");
    }

    fn consolidate_encoded_deltas(deltas: Vec<(Vec<u8>, i64)>) -> HashMap<Vec<u8>, i64> {
        let mut map = HashMap::new();
        for (row, diff) in deltas {
            let next = map.get(&row).copied().unwrap_or(0i64).saturating_add(diff);
            if next == 0 {
                map.remove(&row);
            } else {
                map.insert(row, next);
            }
        }
        map
    }

    fn required_mask(
        requirements: &[PlanSourceRequirements],
        definition: &SourceDefinition,
        source_name: &str,
    ) -> Arc<[bool]> {
        let requirement = requirements
            .iter()
            .find(|requirement| requirement.source_name == source_name)
            .unwrap_or_else(|| panic!("missing source requirement for {source_name}"));
        let mut mask = vec![false; definition.columns().len()];
        for column_idx in &requirement.required_columns {
            mask[*column_idx] = true;
        }
        Arc::from(mask)
    }

    fn encode_event(decoder: &SourceRowDecoder, payload: Value, source: &str) -> Vec<u8> {
        let event = SourceEvent::new(source, payload);
        decoder
            .encode_row_key(&event)
            .expect("encode source event")
            .0
    }

    fn bid_event_payload(auction: i64, bidder: i64, price: i64) -> Value {
        json!({
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "channel",
            "url": "https://example.invalid/bid",
            "date_time": 1_700_000_000_000i64,
            "extra": "extra"
        })
    }

    fn auction_event_payload(id: i64, seller: i64, category: i64) -> Value {
        json!({
            "id": id,
            "item_name": "item",
            "description": "description",
            "initial_bid": 1i64,
            "reserve": 2i64,
            "seller": seller,
            "category": category,
            "expires": 1_700_000_000_000i64,
            "date_time": 1_700_000_000_000i64,
            "extra": "extra"
        })
    }

    fn nexmark_bid_source_definition() -> SourceDefinition {
        SourceDefinition::new(
            "nexmark_bid",
            vec![
                SourceColumn::new("auction", SourceDataType::Int64),
                SourceColumn::new("bidder", SourceDataType::Int64),
                SourceColumn::new("price", SourceDataType::Int64),
                SourceColumn::new("channel", SourceDataType::Utf8),
                SourceColumn::new("url", SourceDataType::Utf8),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("bid definition")
    }

    fn nexmark_auction_source_definition() -> SourceDefinition {
        SourceDefinition::new(
            "nexmark_auction",
            vec![
                SourceColumn::new("id", SourceDataType::Int64),
                SourceColumn::new("item_name", SourceDataType::Utf8),
                SourceColumn::new("description", SourceDataType::Utf8),
                SourceColumn::new("initial_bid", SourceDataType::Int64),
                SourceColumn::new("reserve", SourceDataType::Int64),
                SourceColumn::new("seller", SourceDataType::Int64),
                SourceColumn::new("category", SourceDataType::Int64),
                SourceColumn::new("expires", SourceDataType::TimestampMillis),
                SourceColumn::new("date_time", SourceDataType::TimestampMillis),
                SourceColumn::new("extra", SourceDataType::Utf8),
            ],
        )
        .expect("auction definition")
    }

    async fn test_db(name: &str) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open(name, store).await.expect("open SlateDB"))
    }
}
