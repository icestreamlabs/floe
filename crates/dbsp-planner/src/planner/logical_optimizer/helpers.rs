use super::*;

pub(super) fn projection_with_schema(
    exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: Arc<DFSchema>,
) -> DataFusionResult<Projection> {
    Projection::try_new(exprs.clone(), Arc::clone(&input))?;
    Projection::try_new_with_schema(exprs, input, schema)
}

pub(super) fn aggregate_with_schema(
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: Arc<DFSchema>,
) -> DataFusionResult<Aggregate> {
    Aggregate::try_new(Arc::clone(&input), group_expr.clone(), aggr_expr.clone())?;
    Aggregate::try_new_with_schema(input, group_expr, aggr_expr, schema)
}

pub(super) fn rewrite_expr_through_projection(
    expr: Expr,
    projection: &Projection,
    reject_unsafe_replacements: bool,
) -> DataFusionResult<Option<Expr>> {
    let mut can_rewrite = true;
    let rewritten = expr
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(index) = projection.schema.maybe_index_of_column(&column) else {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                let replacement = projection_expr_value(&projection.expr[index]);
                if reject_unsafe_replacements
                    && !is_safe_projection_replacement(
                        &replacement,
                        projection.input.schema().as_ref(),
                    )
                {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                }
                Ok(Transformed::yes(replacement))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;

    Ok(can_rewrite.then_some(rewritten))
}

pub(super) struct PrunedAggregate {
    pub(super) aggregate: Aggregate,
    pub(super) old_column_indices: BTreeMap<Column, usize>,
    pub(super) old_to_new_indices: BTreeMap<usize, usize>,
}

pub(super) fn prune_aggregate_for_expressions(
    aggregate: &Aggregate,
    required_expressions: &[&Expr],
) -> DataFusionResult<Option<PrunedAggregate>> {
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(None);
    }

    let group_count = aggregate.group_expr.len();
    let mut required_aggregate_indices = BTreeSet::new();
    let mut old_column_indices = BTreeMap::new();

    for expr in required_expressions {
        for column in expr.column_refs() {
            let Some(index) = aggregate.schema.maybe_index_of_column(column) else {
                return Ok(None);
            };
            old_column_indices.insert(column.clone(), index);
            if index >= group_count {
                required_aggregate_indices.insert(index - group_count);
            }
        }
    }

    if required_aggregate_indices.len() == aggregate.aggr_expr.len() {
        return Ok(None);
    }
    if group_count == 0 && required_aggregate_indices.is_empty() {
        return Ok(None);
    }

    let mut old_to_new_indices = BTreeMap::new();
    for group_idx in 0..group_count {
        old_to_new_indices.insert(group_idx, group_idx);
    }

    let mut aggr_expr = Vec::with_capacity(required_aggregate_indices.len());
    for (idx, expr) in aggregate.aggr_expr.iter().enumerate() {
        if required_aggregate_indices.contains(&idx) {
            old_to_new_indices.insert(group_count + idx, group_count + aggr_expr.len());
            aggr_expr.push(expr.clone());
        }
    }

    let aggregate = Aggregate::try_new(
        Arc::clone(&aggregate.input),
        aggregate.group_expr.clone(),
        aggr_expr,
    )?;
    Ok(Some(PrunedAggregate {
        aggregate,
        old_column_indices,
        old_to_new_indices,
    }))
}

pub(super) fn rewrite_exprs_for_pruned_aggregate(
    expressions: &[Expr],
    old_column_indices: &BTreeMap<Column, usize>,
    old_to_new_indices: &BTreeMap<usize, usize>,
    aggregate: &Aggregate,
) -> DataFusionResult<Vec<Expr>> {
    expressions
        .iter()
        .cloned()
        .map(|expr| {
            rewrite_expr_for_pruned_aggregate(
                expr,
                old_column_indices,
                old_to_new_indices,
                aggregate,
            )
        })
        .collect()
}

pub(super) fn rewrite_expr_for_pruned_aggregate(
    expr: Expr,
    old_column_indices: &BTreeMap<Column, usize>,
    old_to_new_indices: &BTreeMap<usize, usize>,
    aggregate: &Aggregate,
) -> DataFusionResult<Expr> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => {
            let Some(old_index) = old_column_indices.get(&column).copied() else {
                return Ok(Transformed::no(Expr::Column(column)));
            };
            let Some(new_index) = old_to_new_indices.get(&old_index).copied() else {
                return Ok(Transformed::no(Expr::Column(column)));
            };
            let (_, field) = aggregate.schema.qualified_field(new_index);
            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                field.name().clone(),
            ))))
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
}

pub(super) fn rewrite_expr_through_aggregate_group_keys(
    expr: Expr,
    aggregate: &Aggregate,
) -> DataFusionResult<Option<Expr>> {
    let mut can_rewrite = true;
    let rewritten = expr
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(index) = aggregate.schema.maybe_index_of_column(&column) else {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                if index >= aggregate.group_expr.len() {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                }
                let replacement = projection_expr_value(&aggregate.group_expr[index]);
                if !is_safe_projection_replacement(&replacement, aggregate.input.schema().as_ref())
                {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                }
                Ok(Transformed::yes(replacement))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;

    Ok(can_rewrite.then_some(rewritten))
}

pub(super) fn is_safe_projection_replacement(expr: &Expr, input_schema: &DFSchema) -> bool {
    !expr.is_volatile()
        && expr
            .column_refs()
            .iter()
            .all(|column| input_schema.maybe_index_of_column(column).is_some())
        && !expr
            .exists(|expr| {
                Ok(matches!(
                    expr,
                    Expr::AggregateFunction(_)
                        | Expr::WindowFunction(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::ScalarSubquery(_)
                ))
            })
            .expect("expression safety check is infallible")
}

pub(super) fn projection_expr_value(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => alias.expr.as_ref().clone(),
        other => other.clone(),
    }
}

pub(super) fn rewrite_expr_through_subquery_alias(
    expr: Expr,
    alias: &SubqueryAlias,
) -> DataFusionResult<Option<Expr>> {
    let mut can_rewrite = true;
    let rewritten = expr
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(index) = alias.schema.maybe_index_of_column(&column) else {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                let (_, field) = alias.schema.qualified_field(index);
                if field.name() != &column.name {
                    can_rewrite = false;
                    return Ok(Transformed::no(Expr::Column(column)));
                }
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    field.name().clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;

    Ok(can_rewrite.then_some(rewritten))
}

pub(super) fn alias_exprs_to_schema(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            let (relation, field) = schema.qualified_field(idx);
            Expr::Alias(Alias::new(
                strip_alias(expr),
                relation.cloned(),
                field.name().clone(),
            ))
        })
        .collect()
}

pub(super) fn strip_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

pub(super) fn is_identity_projection(projection: &Projection) -> bool {
    let input_schema = projection.input.schema();
    if projection.expr.len() != input_schema.fields().len()
        || projection.schema.fields().len() != input_schema.fields().len()
    {
        return false;
    }

    for (idx, expr) in projection.expr.iter().enumerate() {
        let Expr::Column(column) = expr else {
            return false;
        };
        let Ok(input_idx) = input_schema.index_of_column(column) else {
            return false;
        };
        if input_idx != idx
            || projection.schema.qualified_field(idx) != input_schema.qualified_field(idx)
        {
            return false;
        }
    }

    true
}

pub(super) fn split_conjuncts(expr: Expr) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);
    conjuncts
}

pub(super) fn collect_conjuncts(expr: Expr, conjuncts: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            collect_conjuncts(*left, conjuncts);
            collect_conjuncts(*right, conjuncts);
        }
        other => conjuncts.push(other),
    }
}

pub(super) fn and_expr(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    })
}

pub(super) fn can_duplicate_expressions(
    exprs: &[Expr],
    input_count: usize,
    config: &PlannerConfig,
) -> bool {
    if input_count > config.optimizer_max_duplicated_inputs() {
        return false;
    }

    let expr_nodes = exprs.iter().map(expr_node_count).sum::<usize>();
    expr_nodes.saturating_mul(input_count) <= config.optimizer_max_duplicated_expr_nodes()
}

pub(super) fn expr_node_count(expr: &Expr) -> usize {
    let mut count = 0;
    expr.apply(|_| {
        count += 1;
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("expression node counting is infallible");
    count
}
