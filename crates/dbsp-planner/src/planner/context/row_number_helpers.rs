use super::*;

pub(super) fn extract_row_number_limit(
    predicate: &Expr,
) -> Result<Option<(String, usize)>, PlannerError> {
    let normalized = normalize_expr(predicate.clone())?;
    let Expr::BinaryExpr(binary) = normalized else {
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
