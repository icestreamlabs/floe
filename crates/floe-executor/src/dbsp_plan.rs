use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::common::{
    Column,
    tree_node::{Transformed, TreeNode},
};
use datafusion::logical_expr::{Expr, LogicalPlan};

pub use dbsp::circuit::{
    CircuitNode, CircuitPlan, CircuitPlanner, DbspAggregateFunction, DbspAggregateNode,
    DbspDistinctNode, DbspJoinNode, DbspJoinType, DbspNodeKind, DbspProjectNode, DbspScalarType,
    DbspSelectNode, DbspSourceNode, DbspTopNNode, DbspUnionNode, DbspWindowAggregateNode,
    DbspWindowPolicy, DbspWindowSpec, Field, OrderExpr, PlannerConfig, PlannerError, ProjectItem,
    RowSchema, TableDescriptor, nexmark_auction_alias_table, nexmark_auction_table,
    nexmark_bid_alias_table, nexmark_bid_table, nexmark_person_alias_table, nexmark_person_table,
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
                    let Some((project_input_idx, rebased_input_schema, alias_exprs)) =
                        inlinable_source_project_input(&plan, &plan.nodes[idx].inputs)
                    else {
                        continue;
                    };
                    let items: std::result::Result<Vec<_>, PlannerError> = project
                        .expressions()
                        .iter()
                        .map(|expr| {
                            Ok(ProjectItem {
                                expr: rewrite_project_aliases(
                                    expr.expression().expr().clone(),
                                    &alias_exprs,
                                )?,
                                alias: Some(expr.alias().to_string()),
                            })
                        })
                        .collect();
                    let items = items?;
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
                    let rebased =
                        DbspAggregateNode::try_new(rebased_input_schema, group_keys, aggregates)
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
                    let aggregate = DbspAggregateNode::try_new(
                        rebased_input_schema.clone(),
                        group_keys,
                        aggregates,
                    )
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
                DbspNodeKind::Join(join) => {
                    let mut new_inputs = plan.nodes[idx].inputs.clone();
                    let mut left_schema = Arc::clone(&join.left_schema);
                    let mut right_schema = Arc::clone(&join.right_schema);

                    if let Some((left_input_idx, rebased_left_schema)) =
                        bypassable_identity_source_projection_input(&plan, new_inputs[0])
                    {
                        new_inputs[0] = left_input_idx;
                        left_schema = rebased_left_schema;
                    }
                    if let Some((right_input_idx, rebased_right_schema)) =
                        bypassable_identity_source_projection_input(&plan, new_inputs[1])
                    {
                        new_inputs[1] = right_input_idx;
                        right_schema = rebased_right_schema;
                    }
                    if new_inputs == plan.nodes[idx].inputs {
                        continue;
                    }

                    let key_pairs = join
                        .keys
                        .iter()
                        .map(|key| {
                            (
                                key.left_expression().expr().clone(),
                                key.right_expression().expr().clone(),
                            )
                        })
                        .collect();
                    let rebased = DbspJoinNode::try_new(
                        join.join_type.clone(),
                        left_schema,
                        right_schema,
                        key_pairs,
                        join.residual
                            .as_ref()
                            .map(|residual| residual.expr().clone()),
                    )
                    .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
                    let output_schema = Arc::clone(&rebased.output_schema);
                    (new_inputs, DbspNodeKind::Join(rebased), output_schema)
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
        changed |= normalize_project_input_schemas(&mut plan)?;
    }
    prune_unreachable_nodes(&mut plan);
    Ok(plan)
}

fn normalize_project_input_schemas(plan: &mut CircuitPlan) -> Result<bool, PlannerError> {
    let output_schemas_by_node_id: BTreeMap<usize, Arc<RowSchema>> = plan
        .nodes
        .iter()
        .map(|node| (node.id, Arc::clone(&node.output_schema)))
        .collect();
    let mut changed = false;

    for idx in 0..plan.nodes.len() {
        let DbspNodeKind::Project(project) = plan.nodes[idx].kind.clone() else {
            continue;
        };
        let [input_idx] = plan.nodes[idx].inputs.as_slice() else {
            continue;
        };
        let Some(actual_input_schema) = output_schemas_by_node_id.get(input_idx).cloned() else {
            return Err(PlannerError::UnsupportedPlan(format!(
                "project node {} references missing input node {input_idx}",
                plan.nodes[idx].id
            )));
        };
        if row_schema_eq(
            project.input_schema().as_ref(),
            actual_input_schema.as_ref(),
        ) {
            continue;
        }

        let items = project
            .expressions()
            .iter()
            .map(|expr| ProjectItem {
                expr: expr.expression().expr().clone(),
                alias: Some(expr.alias().to_string()),
            })
            .collect::<Vec<_>>();
        let rebased = DbspProjectNode::try_new(actual_input_schema, items)
            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?;
        let output_schema = Arc::clone(rebased.output_schema());
        plan.nodes[idx].kind = DbspNodeKind::Project(rebased);
        plan.nodes[idx].output_schema = output_schema;
        changed = true;
    }

    Ok(changed)
}

fn row_schema_eq(left: &RowSchema, right: &RowSchema) -> bool {
    left.len() == right.len()
        && left
            .fields()
            .iter()
            .zip(right.fields().iter())
            .all(|(left_field, right_field)| {
                left_field.name == right_field.name
                    && left_field.data_type == right_field.data_type
                    && left_field.nullable == right_field.nullable
            })
}

fn inlinable_source_project_input(
    plan: &CircuitPlan,
    inputs: &[usize],
) -> Option<(usize, Arc<RowSchema>, BTreeMap<String, Expr>)> {
    let [input_idx] = inputs else {
        return None;
    };
    let project_node = plan.node(*input_idx)?;
    let DbspNodeKind::Project(project) = &project_node.kind else {
        return None;
    };
    let project_input_idx = *project_node.inputs.first()?;
    if !is_source_unary_chain(plan, project_input_idx) {
        return None;
    }
    Some((
        project_input_idx,
        Arc::clone(project.input_schema()),
        project_alias_exprs(project),
    ))
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

fn bypassable_identity_source_projection_input(
    plan: &CircuitPlan,
    input_idx: usize,
) -> Option<(usize, Arc<RowSchema>)> {
    let project_node = plan.node(input_idx)?;
    let DbspNodeKind::Project(project) = &project_node.kind else {
        return None;
    };
    if !is_identity_source_projection(project) {
        return None;
    }
    let project_input_idx = *project_node.inputs.first()?;
    if !is_source_unary_chain(plan, project_input_idx) {
        return None;
    }
    Some((project_input_idx, Arc::clone(project.input_schema())))
}

fn project_alias_exprs(project: &DbspProjectNode) -> BTreeMap<String, Expr> {
    project
        .expressions()
        .iter()
        .map(|expr| (expr.alias().to_string(), expr.expression().expr().clone()))
        .collect()
}

fn rewrite_project_aliases(
    expr: Expr,
    alias_exprs: &BTreeMap<String, Expr>,
) -> Result<Expr, PlannerError> {
    let mut rewritten = expr;
    for _ in 0..=alias_exprs.len() {
        let next = rewritten
            .clone()
            .transform_up(|node| match node {
                Expr::Column(column) => match alias_exprs.get(column.name.as_str()) {
                    Some(alias_expr) => Ok(Transformed::yes(alias_expr.clone())),
                    None => Ok(Transformed::no(Expr::Column(column))),
                },
                other => Ok(Transformed::no(other)),
            })
            .map(|result| result.data)
            .map_err(|err| PlannerError::AnalysisError(err.into()))?;
        if next == rewritten {
            return Ok(next);
        }
        rewritten = next;
    }
    Err(PlannerError::UnsupportedPlan(
        "optimizer projection aliases formed a rewrite cycle".to_string(),
    ))
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
        let Some(column_idx) =
            direct_projection_column_index(expr.expression().expr(), project.input_schema())
        else {
            return false;
        };
        let Some(field) = project.input_schema().field(column_idx) else {
            return false;
        };
        expr.alias() == field.name && seen.insert(column_idx)
    })
}

fn is_identity_source_projection(project: &DbspProjectNode) -> bool {
    project.expressions().len() == project.input_schema().len()
        && project.expressions().iter().enumerate().all(|(idx, expr)| {
            match project.input_schema().field(idx) {
                Some(field) => {
                    direct_projection_column_index(expr.expression().expr(), project.input_schema())
                        == Some(idx)
                        && expr.alias() == field.name
                }
                None => false,
            }
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
