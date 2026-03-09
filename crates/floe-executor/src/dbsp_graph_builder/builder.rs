use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use anyhow::{Context, Result, anyhow, bail};
use async_recursion::async_recursion;
use dbsp::collections::CompactionPolicy;
use dbsp::handles::ZSetHandle;
use dbsp::storage::gc::{GcPolicy, SweepStats};
use dbsp::stream::DeltaHandleStream;
use dbsp::{CircuitNode, CircuitPlan, CompactionSchedulerConfig, DbspNodeKind, StreamRetention};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
use crate::dbsp_plan::{ValidatedPlan, validate_dbsp_plan};
use crate::delta_consolidation::ConsolidationMode;
use crate::materialized_view::MaterializedViewRegistry;
use crate::task_events::GraphTaskSender;

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
        let root_stream = self
            .compile_node(
                inputs.plan,
                inputs.plan.root,
                inputs.outer_handle_streams,
                &inputs.cancel,
                &inputs.task_events,
                &mut built,
                &inputs.mv_registry,
                &mut mv_latest,
                inputs.mv_retention,
            )
            .await?;

        if !mv_latest.contains_key(inputs.view_name) {
            let root_node = inputs.plan.node(inputs.plan.root).with_context(|| {
                anyhow!("root node {} missing from circuit plan", inputs.plan.root)
            })?;
            let root_schema = Arc::clone(&root_node.output_schema);
            self.materialize_view(
                inputs.view_name,
                root_schema,
                root_stream,
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
                )
                .await?
            }
            DbspNodeKind::Sink(sink) => {
                let input_idx = first_input(node, "sink")?;
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
                    )
                    .await?;
                self.materialize_view(
                    &sink.name,
                    Arc::clone(sink.input_schema()),
                    upstream,
                    cancel,
                    task_events,
                    mv_registry,
                    mv_latest,
                    mv_retention,
                    self.output_consolidation_mode,
                )
                .await?
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
