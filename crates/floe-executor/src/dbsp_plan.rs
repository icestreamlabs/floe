use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, LogicalPlan};

pub use dbsp::circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspDistinctNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode, DbspScalarType,
    DbspSelectNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, DbspWindowAggregateNode,
    DbspWindowPolicy, DbspWindowSpec, Field, OrderExpr, PlannerConfig, PlannerError, ProjectItem,
    RowSchema,
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
        let plan = self.planner.plan(df_plan)?;
        normalize_optimizer_source_projections(plan)
    }
}

fn normalize_optimizer_source_projections(
    mut plan: CircuitPlan,
) -> Result<CircuitPlan, PlannerError> {
    let mut changed = true;
    while changed {
        changed = false;
        for idx in 0..plan.nodes.len() {
            let (new_inputs, new_kind, new_output_schema) = match plan.nodes[idx].kind.clone() {
                DbspNodeKind::Project(project) => {
                    let Some((project_input_idx, rebased_input_schema)) =
                        bypassable_source_projection_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let items = project
                        .expressions()
                        .iter()
                        .map(|expr| ProjectItem {
                            expr: expr.expression().expr().clone(),
                            alias: Some(expr.alias().to_string()),
                        })
                        .collect();
                    let rebased = DbspProjectNode::try_new(rebased_input_schema, items)
                        .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    (
                        vec![project_input_idx],
                        DbspNodeKind::Project(rebased.clone()),
                        Arc::clone(rebased.output_schema()),
                    )
                }
                DbspNodeKind::Select(select) => {
                    let Some((project_input_idx, rebased_input_schema)) =
                        bypassable_source_projection_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let rebased = DbspSelectNode::try_new(
                        rebased_input_schema.clone(),
                        select.predicate().expression().expr().clone(),
                    )
                    .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    (
                        vec![project_input_idx],
                        DbspNodeKind::Select(rebased),
                        rebased_input_schema,
                    )
                }
                DbspNodeKind::Aggregate(aggregate) => {
                    let Some((project_input_idx, rebased_input_schema)) =
                        bypassable_source_projection_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let group_keys = aggregate
                        .group_keys()
                        .iter()
                        .map(|key| {
                            (
                                key.expression().expr().clone(),
                                Some(key.alias().to_string()),
                            )
                        })
                        .collect();
                    let aggregates = aggregate
                        .aggregates()
                        .iter()
                        .map(|agg| {
                            (
                                agg.function().clone(),
                                agg.expression().map(|expr| expr.expr().clone()),
                                agg.filter().map(|expr| expr.expr().clone()),
                                agg.distinct(),
                                Some(agg.alias().to_string()),
                            )
                        })
                        .collect();
                    let rebased = DbspAggregateNode::try_new(rebased_input_schema, group_keys, aggregates)
                        .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    let output_schema = Arc::clone(rebased.output_schema());
                    (
                        vec![project_input_idx],
                        DbspNodeKind::Aggregate(rebased),
                        output_schema,
                    )
                }
                DbspNodeKind::WindowAggregate(window) => {
                    let Some((project_input_idx, rebased_input_schema)) =
                        bypassable_source_projection_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let group_keys = window
                        .aggregate
                        .group_keys()
                        .iter()
                        .map(|key| {
                            (
                                key.expression().expr().clone(),
                                Some(key.alias().to_string()),
                            )
                        })
                        .collect();
                    let aggregates = window
                        .aggregate
                        .aggregates()
                        .iter()
                        .map(|agg| {
                            (
                                agg.function().clone(),
                                agg.expression().map(|expr| expr.expr().clone()),
                                agg.filter().map(|expr| expr.expr().clone()),
                                agg.distinct(),
                                Some(agg.alias().to_string()),
                            )
                        })
                        .collect();
                    let aggregate =
                        DbspAggregateNode::try_new(rebased_input_schema.clone(), group_keys, aggregates)
                            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    let window_spec = DbspWindowSpec::try_new(
                        window.window.policy.clone(),
                        window.window.time_expression.expr().clone(),
                        rebased_input_schema,
                        window.window.allowed_lateness_ms,
                    )
                    .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    (
                        vec![project_input_idx],
                        DbspNodeKind::WindowAggregate(DbspWindowAggregateNode {
                            aggregate,
                            window: window_spec,
                        }),
                        plan.nodes[idx].output_schema.clone(),
                    )
                }
                DbspNodeKind::TopN(topn) => {
                    let Some((project_input_idx, rebased_input_schema)) =
                        bypassable_source_projection_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let partition_by = topn
                        .partition_by()
                        .iter()
                        .map(|expr| expr.expr().clone())
                        .collect();
                    let order_by = topn
                        .order_by()
                        .iter()
                        .map(|expr| {
                            OrderExpr::try_new(
                                expr.expression().expr().clone(),
                                rebased_input_schema.clone(),
                                expr.ascending(),
                                expr.nulls_first(),
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    let rebased = DbspTopNNode::try_new(
                        rebased_input_schema.clone(),
                        partition_by,
                        order_by,
                        topn.limit(),
                        topn.offset(),
                    )
                    .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    (
                        vec![project_input_idx],
                        DbspNodeKind::TopN(rebased),
                        rebased_input_schema,
                    )
                }
                _ => continue,
            };
            if plan.nodes[idx].inputs != new_inputs {
                plan.nodes[idx].inputs = new_inputs;
                plan.nodes[idx].kind = new_kind;
                plan.nodes[idx].output_schema = new_output_schema;
                changed = true;
            }
        }
    }
    prune_unreachable_nodes(&mut plan);
    Ok(plan)
}

fn bypassable_source_projection_input(
    plan: &CircuitPlan,
    inputs: &[usize],
) -> Option<(usize, Arc<RowSchema>)> {
    let [input_idx] = inputs else {
        return None;
    };
    let project_node = plan.node(*input_idx)?;
    let DbspNodeKind::Project(project) = &project_node.kind else {
        return None;
    };
    if !is_simple_source_column_projection(project) {
        return None;
    }
    let project_input_idx = *project_node.inputs.first()?;
    if !is_source_unary_chain(plan, project_input_idx) {
        return None;
    }
    Some((project_input_idx, Arc::clone(project.input_schema())))
}

fn is_source_unary_chain(plan: &CircuitPlan, node_idx: usize) -> bool {
    let Some(node) = plan.node(node_idx) else {
        return false;
    };
    match &node.kind {
        DbspNodeKind::Source(_) => true,
        DbspNodeKind::Select(_) | DbspNodeKind::Passthrough => node
            .inputs
            .first()
            .copied()
            .is_some_and(|input| is_source_unary_chain(plan, input)),
        DbspNodeKind::Project(project) if is_simple_source_column_projection(project) => node
            .inputs
            .first()
            .copied()
            .is_some_and(|input| is_source_unary_chain(plan, input)),
        _ => false,
    }
}

fn is_simple_source_column_projection(project: &DbspProjectNode) -> bool {
    let mut seen = HashSet::new();
    project.expressions().iter().all(|expr| {
        let Some(column_idx) = direct_projection_column_index(expr.expression().expr(), project.input_schema()) else {
            return false;
        };
        let Some(field) = project.input_schema().field(column_idx) else {
            return false;
        };
        expr.alias() == field.name && seen.insert(column_idx)
    })
}

fn direct_projection_column_index(expr: &Expr, schema: &RowSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => resolve_schema_column_index(schema, column),
        Expr::Alias(alias) => direct_projection_column_index(alias.expr.as_ref(), schema),
        _ => None,
    }
}

fn resolve_schema_column_index(schema: &RowSchema, column: &Column) -> Option<usize> {
    let _ = &column.relation;
    schema.field_index(column.name.as_str())
}

fn prune_unreachable_nodes(plan: &mut CircuitPlan) {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![plan.root];
    while let Some(node_id) = pending.pop() {
        if !reachable.insert(node_id) {
            continue;
        }
        if let Some(node) = plan.node(node_id) {
            pending.extend(node.inputs.iter().copied());
        }
    }
    plan.nodes.retain(|node| reachable.contains(&node.id));
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
