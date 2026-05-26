use std::sync::Arc;

use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::logical_plan::{Filter, Projection, SubqueryAlias, Union};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result as DataFusionResult};

use super::error::PlannerError;

const MAX_OPTIMIZER_PASSES: usize = 8;

pub(super) fn optimize_logical_plan(plan: &LogicalPlan) -> Result<LogicalPlan, PlannerError> {
    let mut current = plan.clone();
    for _ in 0..MAX_OPTIMIZER_PASSES {
        let transformed = current
            .transform_up(optimize_node)
            .map_err(|err| PlannerError::AnalysisError(err.into()))?;
        current = transformed.data;
        if !transformed.transformed {
            return Ok(current);
        }
    }
    Ok(current)
}

fn optimize_node(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Filter(filter) => optimize_filter(filter),
        LogicalPlan::Projection(projection) => optimize_projection(projection),
        LogicalPlan::Union(union) => optimize_union(union),
        other => Ok(Transformed::no(other)),
    }
}

fn optimize_filter(filter: Filter) -> DataFusionResult<Transformed<LogicalPlan>> {
    if let LogicalPlan::Filter(inner) = filter.input.as_ref() {
        let predicate = and_expr(inner.predicate.clone(), filter.predicate);
        let merged = Filter::try_new(predicate, Arc::clone(&inner.input))?;
        return Ok(Transformed::yes(LogicalPlan::Filter(merged)));
    }

    if let LogicalPlan::Projection(projection) = filter.input.as_ref()
        && !matches!(projection.input.as_ref(), LogicalPlan::Window(_))
        && !filter.predicate.is_volatile()
        && let Some(predicate) =
            rewrite_expr_through_projection(filter.predicate.clone(), projection, true)?
    {
        let pushed_filter =
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&projection.input))?);
        let projection = projection_with_schema(
            projection.expr.clone(),
            Arc::new(pushed_filter),
            Arc::clone(&projection.schema),
        )?;
        return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
    }

    if let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref()
        && !filter.predicate.is_volatile()
        && let Some(predicate) =
            rewrite_expr_through_subquery_alias(filter.predicate.clone(), alias)?
    {
        let pushed_filter =
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(&alias.input))?);
        let alias = SubqueryAlias::try_new(Arc::new(pushed_filter), alias.alias.clone())?;
        return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)));
    }

    if let LogicalPlan::Union(union) = filter.input.as_ref()
        && !filter.predicate.is_volatile()
        && let Some(inputs) = union
            .inputs
            .iter()
            .map(|input| {
                Filter::try_new(filter.predicate.clone(), Arc::clone(input))
                    .map(|filter| Arc::new(LogicalPlan::Filter(filter)))
                    .ok()
            })
            .collect::<Option<Vec<_>>>()
    {
        let union = Union::try_new(inputs)?;
        return Ok(Transformed::yes(LogicalPlan::Union(union)));
    }

    Ok(Transformed::no(LogicalPlan::Filter(filter)))
}

fn optimize_projection(projection: Projection) -> DataFusionResult<Transformed<LogicalPlan>> {
    if is_identity_projection(&projection) {
        return Ok(Transformed::yes(projection.input.as_ref().clone()));
    }

    if let LogicalPlan::Projection(inner) = projection.input.as_ref()
        && let Some(exprs) = projection
            .expr
            .iter()
            .map(|expr| rewrite_expr_through_projection(expr.clone(), inner, true))
            .collect::<DataFusionResult<Vec<_>>>()?
            .into_iter()
            .collect::<Option<Vec<_>>>()
    {
        let exprs = alias_exprs_to_schema(exprs, projection.schema.as_ref());
        let merged = projection_with_schema(
            exprs,
            Arc::clone(&inner.input),
            Arc::clone(&projection.schema),
        )?;
        return Ok(Transformed::yes(LogicalPlan::Projection(merged)));
    }

    if let LogicalPlan::Union(union) = projection.input.as_ref()
        && projection.expr.iter().all(|expr| !expr.is_volatile())
        && let Some(inputs) = union
            .inputs
            .iter()
            .map(|input| {
                let exprs =
                    alias_exprs_to_schema(projection.expr.clone(), projection.schema.as_ref());
                projection_with_schema(exprs, Arc::clone(input), Arc::clone(&projection.schema))
                    .map(|projection| Arc::new(LogicalPlan::Projection(projection)))
                    .ok()
            })
            .collect::<Option<Vec<_>>>()
    {
        let union = Union::try_new(inputs)?;
        return Ok(Transformed::yes(LogicalPlan::Union(union)));
    }

    Ok(Transformed::no(LogicalPlan::Projection(projection)))
}

fn optimize_union(union: Union) -> DataFusionResult<Transformed<LogicalPlan>> {
    let mut changed = false;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        if let LogicalPlan::Union(inner) = input.as_ref() {
            changed = true;
            inputs.extend(inner.inputs.iter().cloned());
        } else {
            inputs.push(Arc::clone(input));
        }
    }

    if changed {
        return Union::try_new(inputs)
            .map(LogicalPlan::Union)
            .map(Transformed::yes);
    }

    Ok(Transformed::no(LogicalPlan::Union(union)))
}

fn projection_with_schema(
    exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: Arc<DFSchema>,
) -> DataFusionResult<Projection> {
    Projection::try_new(exprs.clone(), Arc::clone(&input))?;
    Projection::try_new_with_schema(exprs, input, schema)
}

fn rewrite_expr_through_projection(
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

fn is_safe_projection_replacement(expr: &Expr, input_schema: &DFSchema) -> bool {
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

fn projection_expr_value(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => alias.expr.as_ref().clone(),
        other => other.clone(),
    }
}

fn rewrite_expr_through_subquery_alias(
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

fn alias_exprs_to_schema(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
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

fn strip_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

fn is_identity_projection(projection: &Projection) -> bool {
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

fn and_expr(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    })
}
