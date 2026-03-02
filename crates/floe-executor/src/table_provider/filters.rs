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

#[derive(Debug, Clone)]
pub(super) struct PrimaryKeyFilter {
    values: Vec<ScalarValue>,
}

impl PrimaryKeyFilter {
    pub fn matches(&self, value: &ScalarValue) -> bool {
        self.values.iter().any(|candidate| candidate == value)
    }
}

pub(super) fn extract_primary_key_filter(
    filters: &[Expr],
    primary_key_column: Option<&str>,
) -> (Option<PrimaryKeyFilter>, Vec<Expr>) {
    let Some(primary_key_column) = primary_key_column else {
        return (None, filters.to_vec());
    };
    let mut pushed = None;
    let mut retained = Vec::with_capacity(filters.len());
    for expr in filters {
        if let Some(filter) = parse_primary_key_expr(expr, primary_key_column) {
            if pushed.is_none() {
                pushed = Some(filter);
            }
            continue;
        }
        retained.push(expr.clone());
    }
    (pushed, retained)
}

pub(super) fn parse_primary_key_expr(
    expr: &Expr,
    primary_key_column: &str,
) -> Option<PrimaryKeyFilter> {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
    {
        if is_named_column(binary.left.as_ref(), primary_key_column) {
            return literal_to_scalar(binary.right.as_ref()).map(|value| PrimaryKeyFilter {
                values: vec![value],
            });
        }
        if is_named_column(binary.right.as_ref(), primary_key_column) {
            return literal_to_scalar(binary.left.as_ref()).map(|value| PrimaryKeyFilter {
                values: vec![value],
            });
        }
    }

    if let Expr::InList(in_list) = expr
        && !in_list.negated
        && is_named_column(in_list.expr.as_ref(), primary_key_column)
    {
        let mut values = Vec::with_capacity(in_list.list.len());
        for item in &in_list.list {
            values.push(literal_to_scalar(item)?);
        }
        if !values.is_empty() {
            return Some(PrimaryKeyFilter { values });
        }
    }
    None
}

fn is_mv_version_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(col) if col.name == MV_VERSION_COLUMN)
}

fn is_named_column(expr: &Expr, column_name: &str) -> bool {
    matches!(expr, Expr::Column(col) if col.name == column_name)
}

fn literal_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

fn literal_to_scalar(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;

    #[test]
    fn parses_primary_key_equality_filter() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("id"))),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(42)), None)),
        ));
        let filter = parse_primary_key_expr(&expr, "id").expect("pk eq filter");
        assert!(filter.matches(&ScalarValue::Int64(Some(42))));
        assert!(!filter.matches(&ScalarValue::Int64(Some(7))));
    }

    #[test]
    fn parses_primary_key_in_list_filter() {
        let expr = Expr::InList(datafusion::logical_expr::expr::InList {
            expr: Box::new(Expr::Column(Column::from_name("id"))),
            list: vec![
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                Expr::Literal(ScalarValue::Int64(Some(2)), None),
            ],
            negated: false,
        });
        let filter = parse_primary_key_expr(&expr, "id").expect("pk in filter");
        assert!(filter.matches(&ScalarValue::Int64(Some(1))));
        assert!(filter.matches(&ScalarValue::Int64(Some(2))));
        assert!(!filter.matches(&ScalarValue::Int64(Some(3))));
    }
}
