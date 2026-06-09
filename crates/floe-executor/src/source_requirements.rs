use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use anyhow::{Result, anyhow, bail};
use dbsp::{CircuitNode, CircuitPlan, DbspNodeKind, RowSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSourceRequirements {
    pub source_name: String,
    pub required_columns: Vec<usize>,
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
                    .entry(source.table.source_name().to_string())
                    .or_default()
                    .extend(required_columns);
            }
            DbspNodeKind::Empty(_) | DbspNodeKind::OneRow(_) | DbspNodeKind::Values(_) => {}
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
                split_join_output_required_columns(
                    join,
                    &required_columns,
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
                if let Some(range) = &join.range {
                    add_required_expression_columns(
                        range.left_lower_expression(),
                        join.left_schema.as_ref(),
                        &mut left_columns,
                    )?;
                    add_required_expression_columns(
                        range.left_upper_expression(),
                        join.left_schema.as_ref(),
                        &mut left_columns,
                    )?;
                    add_required_expression_columns(
                        range.right_key_expression(),
                        join.right_schema.as_ref(),
                        &mut right_columns,
                    )?;
                }
                if let Some(asof) = &join.asof {
                    add_required_expression_columns(
                        asof.left_timestamp_expression(),
                        join.left_schema.as_ref(),
                        &mut left_columns,
                    )?;
                    add_required_expression_columns(
                        asof.right_timestamp_expression(),
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
            DbspNodeKind::Aggregate(aggregate) => {
                let input_idx = first_input(node, "aggregate")?;
                let mut input_columns = BTreeSet::new();
                for group_key in aggregate.group_keys() {
                    add_required_expression_columns(
                        group_key.expression(),
                        aggregate.input_schema().as_ref(),
                        &mut input_columns,
                    )?;
                }
                let group_key_count = aggregate.group_keys().len();
                for column_idx in required_columns {
                    let Some(aggregate_idx) = column_idx.checked_sub(group_key_count) else {
                        continue;
                    };
                    let aggregate_expr =
                        aggregate.aggregates().get(aggregate_idx).ok_or_else(|| {
                            anyhow!(
                                "required output column {column_idx} out of bounds for aggregate node"
                            )
                        })?;
                    if let Some(expr) = aggregate_expr.expression() {
                        add_required_expression_columns(
                            expr,
                            aggregate.input_schema().as_ref(),
                            &mut input_columns,
                        )?;
                    }
                    if let Some(filter) = aggregate_expr.filter() {
                        add_required_expression_columns(
                            filter,
                            aggregate.input_schema().as_ref(),
                            &mut input_columns,
                        )?;
                    }
                }
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::WindowAggregate(window) => {
                let input_idx = first_input(node, "window aggregate")?;
                let aggregate = &window.aggregate;
                let mut input_columns = BTreeSet::new();
                add_required_expression_columns(
                    &window.window.time_expression,
                    aggregate.input_schema().as_ref(),
                    &mut input_columns,
                )?;
                for group_key in aggregate.group_keys() {
                    add_required_expression_columns(
                        group_key.expression(),
                        aggregate.input_schema().as_ref(),
                        &mut input_columns,
                    )?;
                }
                let group_key_count = aggregate.group_keys().len();
                let aggregate_output_offset = 2 + group_key_count;
                for column_idx in required_columns {
                    let Some(aggregate_idx) = column_idx.checked_sub(aggregate_output_offset)
                    else {
                        continue;
                    };
                    let aggregate_expr =
                        aggregate.aggregates().get(aggregate_idx).ok_or_else(|| {
                            anyhow!(
                                "required output column {column_idx} out of bounds for window aggregate node"
                            )
                        })?;
                    if let Some(expr) = aggregate_expr.expression() {
                        add_required_expression_columns(
                            expr,
                            aggregate.input_schema().as_ref(),
                            &mut input_columns,
                        )?;
                    }
                    if let Some(filter) = aggregate_expr.filter() {
                        add_required_expression_columns(
                            filter,
                            aggregate.input_schema().as_ref(),
                            &mut input_columns,
                        )?;
                    }
                }
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::TopN(topn) => {
                let input_idx = first_input(node, "topn")?;
                let mut input_columns = required_columns;
                for partition_expr in topn.partition_by() {
                    add_required_expression_columns(
                        partition_expr,
                        topn.output_schema().as_ref(),
                        &mut input_columns,
                    )?;
                }
                for order_expr in topn.order_by() {
                    add_required_expression_columns(
                        order_expr.expression(),
                        topn.output_schema().as_ref(),
                        &mut input_columns,
                    )?;
                }
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::Union(_) => {
                if node.inputs.is_empty() {
                    bail!("union source requirement analysis expected at least one input");
                }
                for input_idx in &node.inputs {
                    if extend_required_columns(
                        &mut required_columns_by_node,
                        *input_idx,
                        required_columns.clone(),
                    ) {
                        pending.push_back(*input_idx);
                    }
                }
            }
            DbspNodeKind::Distinct(distinct) => {
                let input_idx = first_input(node, "distinct")?;
                let input_columns = (0..distinct.output_schema().len()).collect();
                if extend_required_columns(&mut required_columns_by_node, input_idx, input_columns)
                {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::Passthrough => {
                let input_idx = first_input(node, "passthrough")?;
                if extend_required_columns(
                    &mut required_columns_by_node,
                    input_idx,
                    required_columns,
                ) {
                    pending.push_back(input_idx);
                }
            }
            DbspNodeKind::Sink(_) => {
                let input_idx = first_input(node, "sink")?;
                if extend_required_columns(
                    &mut required_columns_by_node,
                    input_idx,
                    required_columns,
                ) {
                    pending.push_back(input_idx);
                }
            }
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

fn first_input(node: &CircuitNode, label: &str) -> Result<usize> {
    node.inputs
        .first()
        .copied()
        .ok_or_else(|| anyhow!("{label} node missing required input"))
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
        let qualified = column.flat_name();
        let column_idx = input_schema
            .field_index(&qualified)
            .or_else(|| input_schema.field_index(column.name.as_str()))
            .ok_or_else(|| anyhow!("column '{}' not found in input schema", qualified))?;
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

fn split_join_output_required_columns(
    join: &dbsp::DbspJoinNode,
    columns: &BTreeSet<usize>,
    left_columns: &mut BTreeSet<usize>,
    right_columns: &mut BTreeSet<usize>,
) -> Result<()> {
    match join.join_type {
        dbsp::DbspJoinType::Inner
        | dbsp::DbspJoinType::LeftOuter
        | dbsp::DbspJoinType::RightOuter
        | dbsp::DbspJoinType::FullOuter => split_join_required_columns(
            columns,
            join.left_schema.len(),
            left_columns,
            right_columns,
        ),
        dbsp::DbspJoinType::LeftSemi | dbsp::DbspJoinType::LeftAnti => {
            for column_idx in columns {
                if *column_idx >= join.left_schema.len() {
                    bail!(
                        "left semi/anti join output column {column_idx} out of bounds for left width {}",
                        join.left_schema.len()
                    );
                }
                left_columns.insert(*column_idx);
            }
            Ok(())
        }
        dbsp::DbspJoinType::RightSemi | dbsp::DbspJoinType::RightAnti => {
            for column_idx in columns {
                if *column_idx >= join.right_schema.len() {
                    bail!(
                        "right semi/anti join output column {column_idx} out of bounds for right width {}",
                        join.right_schema.len()
                    );
                }
                right_columns.insert(*column_idx);
            }
            Ok(())
        }
    }
}
