use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion_common::{
    Column, DFSchema,
    tree_node::{Transformed, TreeNode},
};

use dbsp_circuit::RowSchema;
use dbsp_circuit::circuit::plan::DbspAggregateFunction;

use super::error::PlannerError;

type JoinKeysAndResidual = (Vec<(Expr, Expr)>, Option<Expr>);
pub(super) struct RangeJoinExpressions {
    pub right_key: Expr,
    pub left_lower: Expr,
    pub left_upper: Expr,
}
pub(super) struct AsofJoinExpressions {
    pub left_timestamp: Expr,
    pub right_timestamp: Expr,
}
pub(super) type AggregateExprSpec = (
    DbspAggregateFunction,
    Option<Expr>,
    Option<Expr>,
    bool,
    Option<String>,
);

pub(super) fn normalize_expr(expr: Expr) -> Result<Expr, PlannerError> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
            column.name.clone(),
        )))),
        Expr::OuterReferenceColumn(data_type, column) => Ok(Transformed::yes(
            Expr::OuterReferenceColumn(data_type, Column::new_unqualified(column.name.clone())),
        )),
        other => Ok(Transformed::no(other)),
    })
    .map(|result| result.data)
    .map_err(|err| PlannerError::AnalysisError(err.into()))
}

pub(super) fn combine_filters(filters: Vec<Expr>) -> Option<Expr> {
    let mut iter = filters.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(acc),
            op: Operator::And,
            right: Box::new(expr),
        })
    }))
}

pub(super) fn extract_join_keys_and_residual(
    expr: &Expr,
) -> Result<JoinKeysAndResidual, PlannerError> {
    let mut key_pairs = Vec::new();
    let mut residuals = Vec::new();
    accumulate_conjuncts(expr, &mut key_pairs, &mut residuals)?;
    Ok((key_pairs, combine_filters(residuals)))
}

pub(super) fn extract_join_keys_and_residual_with_logical_schemas(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: &DFSchema,
    right_logical_schema: &DFSchema,
) -> Result<JoinKeysAndResidual, PlannerError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts_preserving_qualifiers(expr, &mut conjuncts);

    let mut key_pairs = Vec::new();
    let mut residuals = Vec::new();
    for conjunct in conjuncts {
        if let Some((left_key, right_key)) = join_key_candidate(
            &conjunct,
            left_schema,
            right_schema,
            Some(left_logical_schema),
            Some(right_logical_schema),
        )? {
            key_pairs.push((left_key, right_key));
        } else {
            residuals.push(normalize_expr(conjunct)?);
        }
    }

    Ok((key_pairs, combine_filters(residuals)))
}

pub(super) fn extract_range_join_and_residual(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
) -> Result<(Option<RangeJoinExpressions>, Option<Expr>), PlannerError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts)?;

    let mut lower_bounds = Vec::new();
    let mut upper_bounds = Vec::new();
    let mut residuals = Vec::new();
    for conjunct in conjuncts {
        if let Some(bound) = range_bound_candidate(&conjunct, left_schema, right_schema)? {
            match bound.kind {
                RangeBoundKind::LowerInclusive => lower_bounds.push(bound),
                RangeBoundKind::UpperExclusive => upper_bounds.push(bound),
            }
        } else {
            residuals.push(conjunct);
        }
    }

    let mut selected: Option<(usize, usize)> = None;
    for (lower_idx, lower) in lower_bounds.iter().enumerate() {
        for (upper_idx, upper) in upper_bounds.iter().enumerate() {
            if lower.right_key == upper.right_key {
                if selected.is_some() {
                    return Err(PlannerError::UnsupportedJoin(
                        "range joins require exactly one matching lower/upper bound pair"
                            .to_string(),
                    ));
                }
                selected = Some((lower_idx, upper_idx));
            }
        }
    }

    let Some((lower_idx, upper_idx)) = selected else {
        residuals.extend(lower_bounds.into_iter().map(|bound| bound.original));
        residuals.extend(upper_bounds.into_iter().map(|bound| bound.original));
        return Ok((None, combine_filters(residuals)));
    };

    for (idx, bound) in lower_bounds.iter().enumerate() {
        if idx != lower_idx {
            residuals.push(bound.original.clone());
        }
    }
    for (idx, bound) in upper_bounds.iter().enumerate() {
        if idx != upper_idx {
            residuals.push(bound.original.clone());
        }
    }

    let lower = lower_bounds.get(lower_idx).ok_or_else(|| {
        PlannerError::UnsupportedJoin("selected lower range bound index was invalid".to_string())
    })?;
    let upper = upper_bounds.get(upper_idx).ok_or_else(|| {
        PlannerError::UnsupportedJoin("selected upper range bound index was invalid".to_string())
    })?;

    Ok((
        Some(RangeJoinExpressions {
            right_key: lower.right_key.clone(),
            left_lower: lower.left_bound.clone(),
            left_upper: upper.left_bound.clone(),
        }),
        combine_filters(residuals),
    ))
}

pub(super) fn extract_asof_join_and_residual(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
) -> Result<(Option<AsofJoinExpressions>, Option<Expr>), PlannerError> {
    extract_asof_join_and_residual_impl(expr, left_schema, right_schema, None, None, true)
}

pub(super) fn extract_asof_join_and_residual_with_logical_schemas(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: &DFSchema,
    right_logical_schema: &DFSchema,
) -> Result<(Option<AsofJoinExpressions>, Option<Expr>), PlannerError> {
    extract_asof_join_and_residual_impl(
        expr,
        left_schema,
        right_schema,
        Some(left_logical_schema),
        Some(right_logical_schema),
        false,
    )
}

fn extract_asof_join_and_residual_impl(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: Option<&DFSchema>,
    right_logical_schema: Option<&DFSchema>,
    normalize_residuals: bool,
) -> Result<(Option<AsofJoinExpressions>, Option<Expr>), PlannerError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts_preserving_qualifiers(expr, &mut conjuncts);

    let mut selected = None;
    let mut residuals = Vec::new();
    for conjunct in conjuncts {
        if let Some(asof) = asof_candidate(
            &conjunct,
            left_schema,
            right_schema,
            left_logical_schema,
            right_logical_schema,
        )? {
            if selected.is_some() {
                return Err(PlannerError::UnsupportedJoin(
                    "ASOF joins require exactly one right_timestamp <= left_timestamp predicate"
                        .to_string(),
                ));
            }
            selected = Some(asof);
        } else {
            residuals.push(if normalize_residuals {
                normalize_expr(conjunct)?
            } else {
                conjunct
            });
        }
    }

    Ok((selected, combine_filters(residuals)))
}

fn accumulate_conjuncts(
    expr: &Expr,
    key_pairs: &mut Vec<(Expr, Expr)>,
    residuals: &mut Vec<Expr>,
) -> Result<(), PlannerError> {
    let normalized = normalize_expr(expr.clone())?;
    match &normalized {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            accumulate_conjuncts(&binary.left, key_pairs, residuals)?;
            accumulate_conjuncts(&binary.right, key_pairs, residuals)?;
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (&*binary.left, &*binary.right) {
                (Expr::Column(_), Expr::Column(_)) => {
                    key_pairs.push(((*binary.left).clone(), (*binary.right).clone()))
                }
                _ => residuals.push(normalized),
            }
        }
        _ => residuals.push(normalized),
    }
    Ok(())
}

fn join_key_candidate(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: Option<&DFSchema>,
    right_logical_schema: Option<&DFSchema>,
) -> Result<Option<(Expr, Expr)>, PlannerError> {
    let expr = unaliased_expr(expr);
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(None);
    };
    if binary.op != Operator::Eq {
        return Ok(None);
    }

    let left_side = expression_side(
        binary.left.as_ref(),
        left_schema,
        right_schema,
        left_logical_schema,
        right_logical_schema,
    )?;
    let right_side = expression_side(
        binary.right.as_ref(),
        left_schema,
        right_schema,
        left_logical_schema,
        right_logical_schema,
    )?;
    let Some((left_side, right_side)) = left_side.zip(right_side) else {
        return Ok(None);
    };

    let candidate = match (left_side, right_side) {
        (ExpressionSide::Left, ExpressionSide::Right) => Some((
            normalize_expr((*binary.left).clone())?,
            normalize_expr((*binary.right).clone())?,
        )),
        (ExpressionSide::Right, ExpressionSide::Left) => Some((
            normalize_expr((*binary.right).clone())?,
            normalize_expr((*binary.left).clone())?,
        )),
        _ => None,
    };
    Ok(candidate)
}

fn asof_candidate(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: Option<&DFSchema>,
    right_logical_schema: Option<&DFSchema>,
) -> Result<Option<AsofJoinExpressions>, PlannerError> {
    let expr = unaliased_expr(expr);
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(None);
    };

    let left_side = expression_side(
        binary.left.as_ref(),
        left_schema,
        right_schema,
        left_logical_schema,
        right_logical_schema,
    )?;
    let right_side = expression_side(
        binary.right.as_ref(),
        left_schema,
        right_schema,
        left_logical_schema,
        right_logical_schema,
    )?;
    let Some((left_side, right_side)) = left_side.zip(right_side) else {
        return Ok(None);
    };

    let candidate = match (left_side, binary.op, right_side) {
        (ExpressionSide::Right, Operator::LtEq, ExpressionSide::Left) => {
            Some(AsofJoinExpressions {
                right_timestamp: normalize_expr((*binary.left).clone())?,
                left_timestamp: normalize_expr((*binary.right).clone())?,
            })
        }
        (ExpressionSide::Left, Operator::GtEq, ExpressionSide::Right) => {
            Some(AsofJoinExpressions {
                right_timestamp: normalize_expr((*binary.right).clone())?,
                left_timestamp: normalize_expr((*binary.left).clone())?,
            })
        }
        _ => None,
    };
    Ok(candidate)
}

fn flatten_conjuncts_preserving_qualifiers(expr: &Expr, conjuncts: &mut Vec<Expr>) {
    let expr = unaliased_expr(expr);
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            flatten_conjuncts_preserving_qualifiers(&binary.left, conjuncts);
            flatten_conjuncts_preserving_qualifiers(&binary.right, conjuncts);
        }
        _ => conjuncts.push(expr.clone()),
    }
}

fn unaliased_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unaliased_expr(alias.expr.as_ref()),
        other => other,
    }
}

fn flatten_conjuncts(expr: &Expr, conjuncts: &mut Vec<Expr>) -> Result<(), PlannerError> {
    let normalized = normalize_expr(expr.clone())?;
    match &normalized {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            flatten_conjuncts(&binary.left, conjuncts)?;
            flatten_conjuncts(&binary.right, conjuncts)?;
        }
        _ => conjuncts.push(normalized),
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExpressionSide {
    Left,
    Right,
}

#[derive(Clone, Copy)]
enum RangeBoundKind {
    LowerInclusive,
    UpperExclusive,
}

struct RangeBoundCandidate {
    kind: RangeBoundKind,
    right_key: Expr,
    left_bound: Expr,
    original: Expr,
}

fn range_bound_candidate(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
) -> Result<Option<RangeBoundCandidate>, PlannerError> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(None);
    };

    let left_side = expression_side(binary.left.as_ref(), left_schema, right_schema, None, None)?;
    let right_side = expression_side(binary.right.as_ref(), left_schema, right_schema, None, None)?;
    let Some((left_side, right_side)) = left_side.zip(right_side) else {
        return Ok(None);
    };

    let candidate = match (left_side, binary.op, right_side) {
        (ExpressionSide::Right, Operator::GtEq, ExpressionSide::Left) => {
            Some(RangeBoundCandidate {
                kind: RangeBoundKind::LowerInclusive,
                right_key: (*binary.left).clone(),
                left_bound: (*binary.right).clone(),
                original: expr.clone(),
            })
        }
        (ExpressionSide::Left, Operator::LtEq, ExpressionSide::Right) => {
            Some(RangeBoundCandidate {
                kind: RangeBoundKind::LowerInclusive,
                right_key: (*binary.right).clone(),
                left_bound: (*binary.left).clone(),
                original: expr.clone(),
            })
        }
        (ExpressionSide::Right, Operator::Lt, ExpressionSide::Left) => Some(RangeBoundCandidate {
            kind: RangeBoundKind::UpperExclusive,
            right_key: (*binary.left).clone(),
            left_bound: (*binary.right).clone(),
            original: expr.clone(),
        }),
        (ExpressionSide::Left, Operator::Gt, ExpressionSide::Right) => Some(RangeBoundCandidate {
            kind: RangeBoundKind::UpperExclusive,
            right_key: (*binary.right).clone(),
            left_bound: (*binary.left).clone(),
            original: expr.clone(),
        }),
        _ => None,
    };
    Ok(candidate)
}

fn expression_side(
    expr: &Expr,
    left_schema: &RowSchema,
    right_schema: &RowSchema,
    left_logical_schema: Option<&DFSchema>,
    right_logical_schema: Option<&DFSchema>,
) -> Result<Option<ExpressionSide>, PlannerError> {
    let mut side = None;
    for column in expr.column_refs() {
        let (in_left, in_right) = match (left_logical_schema, right_logical_schema) {
            (Some(left_logical_schema), Some(right_logical_schema)) => {
                let in_left = left_logical_schema.is_column_from_schema(column);
                let in_right = right_logical_schema.is_column_from_schema(column);
                if in_left != in_right {
                    (in_left, in_right)
                } else {
                    (
                        left_schema.field_index(column.name.as_str()).is_some(),
                        right_schema.field_index(column.name.as_str()).is_some(),
                    )
                }
            }
            _ => (
                left_schema.field_index(column.name.as_str()).is_some(),
                right_schema.field_index(column.name.as_str()).is_some(),
            ),
        };
        let column_side = match (in_left, in_right) {
            (true, false) => ExpressionSide::Left,
            (false, true) => ExpressionSide::Right,
            _ => return Ok(None),
        };
        match side {
            Some(existing) if existing != column_side => return Ok(None),
            Some(_) => {}
            None => side = Some(column_side),
        }
    }
    Ok(side)
}

pub(super) fn extract_alias(expr: Expr) -> Result<(Expr, Option<String>), PlannerError> {
    if let Expr::Alias(alias) = expr {
        let (inner, existing_alias) = extract_alias(alias.expr.as_ref().clone())?;
        let alias = existing_alias.or_else(|| Some(alias.name.clone()));
        Ok((inner, alias))
    } else {
        Ok((normalize_expr(expr)?, None))
    }
}

#[expect(
    deprecated,
    reason = "DataFusion uses Wildcard for COUNT(*) until the variant is removed"
)]
fn is_wildcard_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Wildcard { .. })
}

pub(super) fn map_aggregate_expr(expr: &Expr) -> Result<AggregateExprSpec, PlannerError> {
    match expr {
        Expr::Alias(alias) => {
            let (function, arg, filter, distinct, existing_alias) =
                map_aggregate_expr(alias.expr.as_ref())?;
            let alias = Some(alias.name.clone()).or(existing_alias);
            Ok((function, arg, filter, distinct, alias))
        }
        Expr::AggregateFunction(func) => {
            if !func.params.order_by.is_empty() {
                return Err(PlannerError::UnsupportedPlan(
                    "ORDER BY within aggregates is not supported".to_string(),
                ));
            }
            if func.params.null_treatment.is_some() {
                return Err(PlannerError::UnsupportedPlan(
                    "NULL treatment modifiers on aggregates are not supported".to_string(),
                ));
            }

            let name = func.func.name().to_ascii_lowercase();
            let agg_function = match name.as_str() {
                "count" => DbspAggregateFunction::Count,
                "sum" => DbspAggregateFunction::Sum,
                "min" => DbspAggregateFunction::Min,
                "max" => DbspAggregateFunction::Max,
                "avg" => DbspAggregateFunction::Avg,
                other => {
                    return Err(PlannerError::UnsupportedPlan(format!(
                        "aggregate function '{other}' is not supported",
                    )));
                }
            };
            if func.params.distinct && agg_function != DbspAggregateFunction::Count {
                return Err(PlannerError::UnsupportedPlan(
                    "DISTINCT is only supported for COUNT aggregates".to_string(),
                ));
            }

            if func.params.args.len() > 1 {
                return Err(PlannerError::UnsupportedPlan(
                    "aggregates with more than one argument are not supported".to_string(),
                ));
            }

            let expression = func
                .params
                .args
                .first()
                .and_then(|arg| (!is_wildcard_expr(arg)).then(|| arg.clone()));

            let expression = match expression {
                Some(expr) => Some(normalize_expr(expr)?),
                None => None,
            };
            let filter = match &func.params.filter {
                Some(expr) => Some(normalize_expr((**expr).clone())?),
                None => None,
            };

            Ok((
                agg_function,
                expression,
                filter,
                func.params.distinct,
                Some(expr.schema_name().to_string()),
            ))
        }
        _ => Err(PlannerError::UnsupportedPlan(
            "aggregate expressions must be aggregate functions".to_string(),
        )),
    }
}
