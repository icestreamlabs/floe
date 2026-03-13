use std::collections::{BTreeSet, HashMap};
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

use super::materialize::DeltaTransformFn;
use super::persistence_policy::{PersistencePolicy, TransientSegmentSpec, TransientSegmentStep};
use super::vectorized_filter_project::VectorizedFilterProjectEvaluator;

/// Orchestrates compilation of a [`CircuitPlan`] into DBSP streams backed by SlateDB.
pub struct DbspGraphBuilder {
    pub(super) bridge: Arc<Mutex<DbspBridge>>,
    ns: GraphNamespace,
    pub(super) watermark: Arc<AtomicI64>,
    output_consolidation_mode: ConsolidationMode,
    pub(super) mv_flush_coalescing: MvFlushCoalescingConfig,
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

pub fn source_batch_journal_root_source_name(plan: &CircuitPlan) -> Option<String> {
    find_transient_source_root_shape(plan, plan.root)
        .ok()
        .flatten()
        .map(|shape| shape.source_name().to_string())
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
