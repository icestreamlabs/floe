use super::*;

#[derive(Default, Clone)]
pub(super) struct GraphNamespace {
    pub(super) graph_id: String,
}

impl GraphNamespace {
    pub(super) fn set_graph_id(&mut self, graph_id: impl Into<String>) {
        self.graph_id = graph_id.into();
    }
}

pub struct LegacyGraphHarnessInputs<'a> {
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

pub struct LegacyGraphHarnessOutputs {
    pub node_streams: HashMap<usize, DeltaHandleStream>,
    pub mv_latest: HashMap<String, (i64, ZSetHandle)>,
    pub required_sources: BTreeSet<String>,
}

pub(super) fn should_compact_transient_helper_state(
    upstream: &TransientSourceHandleStream,
    state_table: Option<&Arc<dyn KeyValueTable>>,
) -> bool {
    // Compact snapshots rewrite all helper state on every input batch. Keep that
    // behavior opt-in so the steady-state path persists only incremental deltas.
    upstream.recoverable() && state_table.is_some() && transient_compact_helper_state_env_enabled()
}

pub(super) fn transient_compact_helper_state_env_enabled() -> bool {
    std::env::var("FLOE_COMPACT_TRANSIENT_HELPER_STATE")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

pub(super) fn first_input(node: &CircuitNode, label: &str) -> Result<usize> {
    node.inputs
        .first()
        .copied()
        .with_context(|| anyhow!("{label} node missing required input"))
}

pub(super) fn join_inputs(node: &CircuitNode) -> Result<(usize, usize)> {
    if node.inputs.len() < 2 {
        bail!("join node requires two inputs");
    }
    Ok((node.inputs[0], node.inputs[1]))
}

pub(super) fn plan_node_output_append_only(plan: &CircuitPlan, node_idx: usize) -> Result<bool> {
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

pub(super) fn fuseable_select_input(
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

pub(super) fn has_single_consumer(plan: &CircuitPlan, node_idx: usize) -> bool {
    plan.nodes()
        .iter()
        .flat_map(|node| node.inputs.iter())
        .filter(|&&input| input == node_idx)
        .count()
        == 1
}

#[derive(Clone)]
pub(super) struct TransientSourceRootMaterialization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) transform: Arc<DeltaTransformFn>,
}

pub(super) struct TransientSourceTopNRootMaterialization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) receiver: TransientMaterializeReceiver,
    pub(super) transform: Option<Arc<DeltaTransformFn>>,
}

pub(super) struct TransientSourceAggregateRootMaterialization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) receiver: TransientMaterializeReceiver,
}

pub(super) struct TransientSourceWindowCountStarRootMaterialization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) receiver: TransientMaterializeReceiver,
}

pub(super) struct TransientSourceWindowAggregateRootMaterialization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) receiver: TransientMaterializeReceiver,
}

pub(super) struct TransientJoinInputOptimization {
    pub(super) source_name: String,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) receiver:
        tokio::sync::mpsc::Receiver<dbsp::join::TransientJoinInputBatch<Vec<u8>, Vec<u8>>>,
}

#[derive(Clone)]
pub(super) struct TransientJoinPipelineRootMaterialization {
    pub(super) left_input_idx: usize,
    pub(super) right_input_idx: usize,
    pub(super) left_source_root: TransientSourceRootMaterialization,
    pub(super) right_source_root: TransientSourceRootMaterialization,
    pub(super) join: dbsp::DbspJoinNode,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) steps: Vec<TransientJoinPipelineStep>,
}

#[derive(Clone)]
pub(super) struct TransientSourceTopNRootShape {
    pub(super) source_root: TransientSourceRootMaterialization,
    pub(super) topn: DbspTopNNode,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) transform: Option<Arc<DeltaTransformFn>>,
    pub(super) output_projection: Option<Arc<Vec<usize>>>,
}

#[derive(Clone)]
pub(super) struct TransientSourceAggregateRootShape {
    pub(super) source_root: TransientSourceRootMaterialization,
    pub(super) aggregate: DbspAggregateNode,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) transform: Arc<DeltaTransformFn>,
}

#[derive(Clone)]
pub(super) struct TransientSourceWindowCountStarRootShape {
    pub(super) source_root: TransientSourceRootMaterialization,
    pub(super) window: dbsp::DbspWindowAggregateNode,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) transform: Option<Arc<DeltaTransformFn>>,
    pub(super) output_projection: Option<TransientWindowCountOutputProjection>,
}

#[derive(Clone)]
pub(super) struct TransientSourceWindowAggregateRootShape {
    pub(super) source_root: TransientSourceRootMaterialization,
    pub(super) window: dbsp::DbspWindowAggregateNode,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) transform: Arc<DeltaTransformFn>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TransientWindowCountKey {
    pub(super) start: i64,
    pub(super) end: i64,
    pub(super) key: Arc<[u8]>,
}

#[derive(Clone, Copy)]
pub(super) enum TransientWindowCountOutputProjection {
    GroupKeyAndCount,
}

pub(super) enum TransientWindowCountUpdates {
    Full(AHashMap<(TransientWindowCountKey, i64), i64>),
    GroupKeyAndCount(AHashMap<(Arc<[u8]>, i64), i64>),
}

impl TransientWindowCountUpdates {
    pub(super) fn new(projection: Option<TransientWindowCountOutputProjection>) -> Self {
        match projection {
            Some(TransientWindowCountOutputProjection::GroupKeyAndCount) => {
                Self::GroupKeyAndCount(AHashMap::new())
            }
            None => Self::Full(AHashMap::new()),
        }
    }

    pub(super) fn merge(&mut self, key: &TransientWindowCountKey, count: i64, delta: i64) {
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
pub(super) enum TransientJoinPipelineStep {
    Transform(Arc<DeltaTransformFn>),
    Aggregate(DbspAggregateNode),
    TopN(DbspTopNNode),
}

pub(super) enum TransientSourceRootShape {
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
    pub(super) fn source_name(&self) -> &str {
        match self {
            Self::Source { source, .. }
            | Self::Select { source, .. }
            | Self::Project { source, .. }
            | Self::FilterMap { source, .. } => source.table.source_name(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransientSourceRootRequirements {
    pub source_name: String,
    pub required_columns: Vec<usize>,
}
