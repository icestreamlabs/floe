use super::*;

pub(super) fn extract_row_number_limit_with_residual(
    predicate: &Expr,
) -> Result<Option<(String, usize, Option<Expr>)>, PlannerError> {
    let normalized = normalize_expr(predicate.clone())?;
    extract_row_number_limit_with_residual_from_normalized(normalized)
}

fn extract_row_number_limit_with_residual_from_normalized(
    expr: Expr,
) -> Result<Option<(String, usize, Option<Expr>)>, PlannerError> {
    if let Some((column, limit)) = extract_direct_row_number_limit(&expr)? {
        return Ok(Some((column, limit, None)));
    }

    let Expr::BinaryExpr(binary) = expr else {
        return Ok(None);
    };
    if binary.op != Operator::And {
        return Ok(None);
    }

    let left = *binary.left;
    let right = *binary.right;
    let left_match = extract_row_number_limit_with_residual_from_normalized(left.clone())?;
    let right_match = extract_row_number_limit_with_residual_from_normalized(right.clone())?;
    match (left_match, right_match) {
        (Some(_), Some(_)) => Err(PlannerError::UnsupportedPlan(
            "only one ROW_NUMBER limit predicate is supported".to_string(),
        )),
        (Some((column, limit, residual)), None) => {
            let mut residuals = Vec::new();
            if let Some(residual) = residual {
                residuals.push(residual);
            }
            residuals.push(right);
            Ok(Some((column, limit, combine_filters(residuals))))
        }
        (None, Some((column, limit, residual))) => {
            let mut residuals = vec![left];
            if let Some(residual) = residual {
                residuals.push(residual);
            }
            Ok(Some((column, limit, combine_filters(residuals))))
        }
        (None, None) => Ok(None),
    }
}

fn extract_direct_row_number_limit(expr: &Expr) -> Result<Option<(String, usize)>, PlannerError> {
    let Expr::BinaryExpr(binary) = expr else {
        return Ok(None);
    };
    let (column, literal, exclusive) = match (&*binary.left, binary.op, &*binary.right) {
        (Expr::Column(column), Operator::LtEq, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, false)
        }
        (Expr::Column(column), Operator::Lt, literal @ Expr::Literal(_, _)) => {
            (column.name.clone(), literal, true)
        }
        _ => return Ok(None),
    };

    let mut limit = literal_to_positive_usize(literal)?;
    if exclusive {
        if limit == 0 {
            return Ok(None);
        }
        limit -= 1;
    }
    if limit == 0 {
        return Ok(None);
    }
    Ok(Some((column, limit)))
}

pub(super) fn projection_expr_matches_rank(expr: &Expr, rank_column: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == rank_column,
        Expr::Alias(alias) => alias.name == rank_column,
        _ => false,
    }
}

pub(super) fn literal_to_positive_usize(expr: &Expr) -> Result<usize, PlannerError> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        ));
    };
    let array = value.to_array().map_err(|_| {
        PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        )
    })?;
    let as_i128 = array_to_i128(array.as_ref()).ok_or_else(|| {
        PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be a positive integer literal".to_string(),
        )
    })?;

    if as_i128 <= 0 {
        return Err(PlannerError::UnsupportedPlan(
            "ROW_NUMBER filter limit must be positive".to_string(),
        ));
    }
    usize::try_from(as_i128).map_err(|_| {
        PlannerError::UnsupportedPlan("ROW_NUMBER filter limit is out of range".to_string())
    })
}

pub(super) fn array_to_i128(array: &dyn Array) -> Option<i128> {
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return (!values.is_null(0)).then(|| i128::from(values.value(0)));
    }
    None
}
