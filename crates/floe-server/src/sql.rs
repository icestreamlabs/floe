use std::collections::HashSet;
use core::ops::ControlFlow;

use bytes::Bytes;
use pgwire::api::results::FieldFormat;
use pgwire::error::PgWireResult;
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableFactor, Value,
    ValueWithSpan, visit_expressions, visit_expressions_mut,
};

use floe_executor::namespaces;

use super::user_error;

pub(crate) fn ensure_select_statement(statement: &Statement) -> PgWireResult<()> {
    match statement {
        Statement::Query(query) if is_select_expr(&query.body) => Ok(()),
        _ => Err(user_error("only SELECT statements are supported")),
    }
}

fn is_select_expr(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(_) => true,
        SetExpr::SetOperation { left, right, .. } => is_select_expr(left) && is_select_expr(right),
        SetExpr::Query(query) => is_select_expr(&query.body),
        _ => false,
    }
}

pub(crate) fn collect_placeholder_indices(statement: &Statement) -> PgWireResult<Vec<usize>> {
    let mut indices = Vec::new();
    let result = visit_expressions(statement, |expr| {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(name),
            ..
        }) = expr
        {
            match parse_placeholder_index(name) {
                Ok(idx) => indices.push(idx),
                Err(err) => return ControlFlow::Break(err),
            }
        }
        ControlFlow::Continue(())
    });
    match result {
        ControlFlow::Continue(_) => Ok(indices),
        ControlFlow::Break(err) => Err(err),
    }
}

fn parse_placeholder_index(name: &str) -> PgWireResult<usize> {
    let trimmed = name.trim_start_matches(['$', '?']);
    if trimmed.is_empty() {
        return Err(user_error(format!("invalid placeholder '{name}'")));
    }
    let idx = trimmed
        .parse::<usize>()
        .map_err(|_| user_error(format!("invalid placeholder '{name}'")))?;
    if idx == 0 {
        return Err(user_error(format!("invalid placeholder '{name}'")));
    }
    Ok(idx)
}

pub(crate) fn decode_parameter_value(
    raw: Option<&Bytes>,
    _format: FieldFormat,
) -> PgWireResult<ValueWithSpan> {
    match raw {
        None => Ok(Value::Null.with_empty_span()),
        Some(bytes) => {
            let text = std::str::from_utf8(bytes.as_ref())
                .map_err(|_| user_error("parameter values must be valid UTF-8"))?;
            Ok(string_to_value(text).with_empty_span())
        }
    }
}

fn string_to_value(input: &str) -> Value {
    if input.parse::<i64>().is_ok() || input.parse::<f64>().is_ok() {
        Value::Number(input.to_string(), false)
    } else if input.eq_ignore_ascii_case("true") {
        Value::Boolean(true)
    } else if input.eq_ignore_ascii_case("false") {
        Value::Boolean(false)
    } else {
        Value::SingleQuotedString(input.to_string())
    }
}

pub(crate) fn substitute_placeholders(
    statement: &mut Statement,
    values: &[ValueWithSpan],
) -> PgWireResult<()> {
    let result = visit_expressions_mut(statement, |expr| {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(name),
            ..
        }) = expr
        {
            let idx = match parse_placeholder_index(name) {
                Ok(idx) => idx,
                Err(err) => return ControlFlow::Break(err),
            };
            if idx == 0 || idx > values.len() {
                return ControlFlow::Break(user_error(format!(
                    "placeholder {name} has no bound value"
                )));
            }
            *expr = Expr::Value(values[idx - 1].clone());
        }
        ControlFlow::Continue(())
    });
    match result {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

pub(crate) fn extract_tables_from_query(query: &Query, names: &mut Vec<String>) {
    extract_tables_from_setexpr(&query.body, names);
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_tables_from_query(&cte.query, names);
        }
    }
}

fn extract_tables_from_setexpr(expr: &SetExpr, names: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                extract_tables_from_table_factor(&table.relation, names);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_setexpr(left, names);
            extract_tables_from_setexpr(right, names);
        }
        SetExpr::Query(query) => extract_tables_from_query(query, names),
        _ => {}
    }
}

fn extract_tables_from_table_factor(factor: &TableFactor, names: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            if let Some(table) = normalize_object_name(name) {
                names.push(table);
            }
        }
        TableFactor::Derived { subquery, .. } => extract_tables_from_query(subquery, names),
        _ => {}
    }
}

fn normalize_object_name(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|Ident { value, .. }| value.clone())
}

pub(crate) fn mv_identifiers_in_sql(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for raw in sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"')) {
        if raw.is_empty() {
            continue;
        }
        if let Some(name) = normalize_identifier(raw)
            && seen.insert(name.clone())
        {
            names.push(name);
        }
    }
    names
}

fn normalize_identifier(raw: &str) -> Option<String> {
    let quoted = raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2;
    let inner = if quoted { &raw[1..raw.len() - 1] } else { raw };
    if inner.is_empty() {
        return None;
    }
    let normalized = if quoted {
        inner.to_string()
    } else {
        inner.to_ascii_lowercase()
    };
    if normalized.starts_with("mv_") && namespaces::materialized_view(&normalized).is_ok() {
        Some(normalized)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_mv_identifiers_in_sql() {
        let sql = r#"SELECT * FROM mv_orders JOIN "mv_Sales" ON mv_orders.id = "mv_Sales".id"#;
        let mut names = mv_identifiers_in_sql(sql);
        names.sort();
        let mut expected = vec!["mv_orders".to_string(), "mv_Sales".to_string()];
        expected.sort();
        assert_eq!(names, expected);
    }
}
