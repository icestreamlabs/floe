use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::logical_expr::{Expr, Operator};

use super::MV_VERSION_COLUMN;

pub(super) fn extract_mv_version_filter(filters: &[Expr]) -> (Option<u64>, Vec<Expr>) {
    let mut as_of_version = None;
    let mut retained = Vec::with_capacity(filters.len());
    for expr in filters {
        if let Some(version) = parse_mv_version_expr(expr) {
            if as_of_version.is_none() {
                as_of_version = Some(version);
            }
            continue;
        }
        retained.push(expr.clone());
    }
    (as_of_version, retained)
}

pub(super) fn parse_mv_version_expr(expr: &Expr) -> Option<u64> {
    if let Expr::BinaryExpr(binary) = expr {
        if binary.op != Operator::Eq {
            return None;
        }
        if is_mv_version_column(binary.left.as_ref()) {
            return literal_to_u64(binary.right.as_ref());
        }
        if is_mv_version_column(binary.right.as_ref()) {
            return literal_to_u64(binary.left.as_ref());
        }
    }
    None
}

fn is_mv_version_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(col) if col.name == MV_VERSION_COLUMN)
}

fn literal_to_u64(expr: &Expr) -> Option<u64> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    let array = value.to_array().ok()?;
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return (!values.is_null(0)).then(|| values.value(0));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return (!values.is_null(0))
            .then(|| values.value(0))
            .filter(|value| *value >= 0)
            .map(|value| value as u64);
    }
    None
}
