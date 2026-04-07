use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, anyhow, bail};
use datafusion::logical_expr::LogicalPlan;

pub use dbsp::circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspDistinctNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode, DbspScalarType,
    DbspSelectNode, DbspSourceNode, DbspUnionNode, DbspWindowAggregateNode, DbspWindowPolicy,
    DbspWindowSpec, Field, OrderExpr, PlannerConfig, PlannerError, ProjectItem, RowSchema,
    TableDescriptor, nexmark_auction_alias_table, nexmark_auction_table, nexmark_bid_alias_table,
    nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
};

use crate::namespaces;

/// Thin wrapper around DBSP's [`CircuitPlanner`] that exposes a planning API within Floe.
pub struct DbspPlanBuilder {
    planner: CircuitPlanner,
}

impl DbspPlanBuilder {
    pub fn new(config: PlannerConfig) -> Self {
        Self {
            planner: CircuitPlanner::new(config),
        }
    }

    pub fn build(&self, df_plan: &LogicalPlan) -> Result<CircuitPlan, PlannerError> {
        self.planner.plan(df_plan)
    }
}

/// Returns a [`PlannerConfig`] pre-populated with Nexmark table descriptors.
pub fn nexmark_config() -> PlannerConfig {
    let mut cfg = PlannerConfig::new();
    cfg.register_table(nexmark_person_table());
    cfg.register_table(nexmark_person_alias_table());
    cfg.register_table(nexmark_auction_table());
    cfg.register_table(nexmark_auction_alias_table());
    cfg.register_table(nexmark_bid_table());
    cfg.register_table(nexmark_bid_alias_table());
    cfg
}

#[derive(Clone, Debug)]
pub struct ValidatedPlan {
    pub plan: CircuitPlan,
    pub required_sources: BTreeSet<String>,
    pub root_node: usize,
    pub root_is_sink: bool,
    pub fan_in_nodes: Vec<usize>,
}

pub fn validate_dbsp_plan(
    plan: &CircuitPlan,
    outer_streams_available: &BTreeSet<String>,
    view_name: &str,
) -> Result<ValidatedPlan> {
    namespaces::materialized_view(view_name)?;
    let root_id = plan.root;
    let root_node = node(plan, root_id)?;
    let root_is_sink = matches!(root_node.kind, DbspNodeKind::Sink(_));
    let topo = topo_order(plan)?;

    let sources = required_sources(plan);
    if let Some(missing) = sources
        .iter()
        .find(|name| !outer_streams_available.contains(*name))
    {
        bail!(
            "source '{missing}' not provided; available sources: {}",
            format_set(outer_streams_available)
        );
    }

    let mut fan_in_nodes = Vec::new();

    for &node_id in &topo {
        let circuit_node = node(plan, node_id)?;
        let input_count = circuit_node.inputs.len();
        if input_count > 1 {
            fan_in_nodes.push(node_id);
        }
        match &circuit_node.kind {
            DbspNodeKind::Source(_) => {
                if input_count != 0 {
                    bail!("node {node_id} → Source expects 0 inputs (found {input_count})");
                }
            }
            DbspNodeKind::Project(_)
            | DbspNodeKind::Select(_)
            | DbspNodeKind::Distinct(_)
            | DbspNodeKind::Aggregate(_)
            | DbspNodeKind::TopN(_)
            | DbspNodeKind::WindowAggregate(_)
            | DbspNodeKind::Passthrough => {
                if input_count != 1 {
                    bail!(
                        "node {node_id} → {} expects 1 input (found {input_count})",
                        kind_name(&circuit_node.kind)
                    );
                }
            }
            DbspNodeKind::Sink(_) => {
                if input_count != 1 {
                    bail!("node {node_id} → Sink expects 1 input (found {input_count})");
                }
            }
            DbspNodeKind::Join(_) => {
                if input_count < 2 {
                    bail!("node {node_id} → Join expects ≥2 inputs (found {input_count})");
                }
            }
            DbspNodeKind::Union(_) => {
                if input_count < 2 {
                    bail!("node {node_id} → Union expects ≥2 inputs (found {input_count})");
                }
            }
        }

        if let DbspNodeKind::Join(join) = &circuit_node.kind {
            let left_width = join.left_schema.len();
            let right_width = join.right_schema.len();
            let output_width = join.output_schema.len();
            if left_width + right_width != output_width {
                bail!(
                    "node {node_id} (Join) output width mismatch: {} + {} ≠ {}",
                    left_width,
                    right_width,
                    output_width
                );
            }
        }
    }

    Ok(ValidatedPlan {
        plan: plan.clone(),
        required_sources: sources,
        root_node: root_id,
        root_is_sink,
        fan_in_nodes,
    })
}

fn node(plan: &CircuitPlan, id: usize) -> Result<&CircuitNode> {
    plan.node(id)
        .ok_or_else(|| anyhow!("node {id} not found in circuit plan"))
}

fn topo_order(plan: &CircuitPlan) -> Result<Vec<usize>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum VisitState {
        Pending,
        Visiting,
        Visited,
    }

    let mut state = BTreeMap::new();
    for circuit_node in &plan.nodes {
        state.insert(circuit_node.id, VisitState::Pending);
    }

    let mut order = Vec::with_capacity(plan.nodes.len());

    fn dfs(
        plan: &CircuitPlan,
        id: usize,
        state: &mut BTreeMap<usize, VisitState>,
        order: &mut Vec<usize>,
    ) -> Result<()> {
        match state.get(&id).copied() {
            Some(VisitState::Visited) => return Ok(()),
            Some(VisitState::Visiting) => {
                bail!("graph has a cycle involving node {id}");
            }
            None => bail!("node {id} not present in circuit plan"),
            Some(VisitState::Pending) => {}
        }

        state.insert(id, VisitState::Visiting);
        let current = node(plan, id)?;
        for input in &current.inputs {
            node(plan, *input)
                .with_context(|| format!("node {id} references missing input {input}"))?;
            dfs(plan, *input, state, order)?;
        }
        state.insert(id, VisitState::Visited);
        order.push(id);
        Ok(())
    }

    dfs(plan, plan.root, &mut state, &mut order)?;
    if let Some((&unreachable, _)) = state
        .iter()
        .find(|(_, visit_state)| **visit_state != VisitState::Visited)
    {
        bail!(
            "node {unreachable} is not reachable from root {}",
            plan.root
        );
    }
    Ok(order)
}

fn required_sources(plan: &CircuitPlan) -> BTreeSet<String> {
    plan.nodes
        .iter()
        .filter_map(|node| match &node.kind {
            DbspNodeKind::Source(source) => Some(source.table.name.to_string()),
            _ => None,
        })
        .collect()
}

fn format_set(values: &BTreeSet<String>) -> String {
    let joined = values.iter().cloned().collect::<Vec<_>>().join(", ");
    format!("{{{joined}}}")
}

fn kind_name(kind: &DbspNodeKind) -> &'static str {
    match kind {
        DbspNodeKind::Source(_) => "Source",
        DbspNodeKind::Select(_) => "Select",
        DbspNodeKind::Project(_) => "Project",
        DbspNodeKind::Join(_) => "Join",
        DbspNodeKind::Aggregate(_) => "Aggregate",
        DbspNodeKind::Distinct(_) => "Distinct",
        DbspNodeKind::WindowAggregate(_) => "WindowAggregate",
        DbspNodeKind::TopN(_) => "TopN",
        DbspNodeKind::Union(_) => "Union",
        DbspNodeKind::Passthrough => "Passthrough",
        DbspNodeKind::Sink(_) => "Sink",
    }
}
