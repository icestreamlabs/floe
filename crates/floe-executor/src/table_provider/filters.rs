use anyhow::Result;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::encoding::{EncodedRowScalar, extract_encoded_row_scalar};

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
    values: Vec<EncodedRowScalar>,
}

impl PrimaryKeyFilter {
    pub fn matches_encoded(&self, value: Option<&EncodedRowScalar>) -> bool {
        value.is_some_and(|value| self.values.iter().any(|candidate| candidate == value))
    }

    pub fn matches_encoded_row(&self, row_key: &[u8], column_index: usize) -> Result<bool> {
        let value = extract_encoded_row_scalar(row_key, column_index)?;
        Ok(self.matches_encoded(value.as_ref()))
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
            return literal_to_encoded_scalar(binary.right.as_ref()).map(|value| {
                PrimaryKeyFilter {
                    values: vec![value],
                }
            });
        }
        if is_named_column(binary.right.as_ref(), primary_key_column) {
            return literal_to_encoded_scalar(binary.left.as_ref()).map(|value| PrimaryKeyFilter {
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
            values.push(literal_to_encoded_scalar(item)?);
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

fn literal_to_encoded_scalar(expr: &Expr) -> Option<EncodedRowScalar> {
    match expr {
        Expr::Literal(value, _) => scalar_to_encoded(value),
        _ => None,
    }
}

fn scalar_to_encoded(value: &ScalarValue) -> Option<EncodedRowScalar> {
    match value {
        ScalarValue::Int64(Some(value)) => Some(EncodedRowScalar::Int64(*value)),
        ScalarValue::Utf8(Some(value)) => Some(EncodedRowScalar::Utf8(value.clone())),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(EncodedRowScalar::TimestampMillis(*value))
        }
        ScalarValue::Boolean(Some(value)) => Some(EncodedRowScalar::Bool(*value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::lit;

    #[test]
    fn parses_primary_key_equality_filter() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("id"))),
            Operator::Eq,
            Box::new(lit(42_i64)),
        ));
        let filter = parse_primary_key_expr(&expr, "id").expect("pk eq filter");
        assert!(filter.matches_encoded(Some(&EncodedRowScalar::Int64(42))));
        assert!(!filter.matches_encoded(Some(&EncodedRowScalar::Int64(7))));
    }

    #[test]
    fn parses_primary_key_in_list_filter() {
        let expr = Expr::InList(datafusion::logical_expr::expr::InList {
            expr: Box::new(Expr::Column(Column::from_name("id"))),
            list: vec![lit(1_i64), lit(2_i64)],
            negated: false,
        });
        let filter = parse_primary_key_expr(&expr, "id").expect("pk in filter");
        assert!(filter.matches_encoded(Some(&EncodedRowScalar::Int64(1))));
        assert!(filter.matches_encoded(Some(&EncodedRowScalar::Int64(2))));
        assert!(!filter.matches_encoded(Some(&EncodedRowScalar::Int64(3))));
    }
}
