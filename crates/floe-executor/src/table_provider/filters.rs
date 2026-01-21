use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;

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
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}
