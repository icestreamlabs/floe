#[cfg(test)]
use std::collections::HashSet;

use pgwire::error::PgWireResult;
use sqlparser::ast::{Ident, ObjectName, Query, SetExpr, Statement, TableFactor};

#[cfg(test)]
use floe_executor::namespaces;

use super::feature_not_supported_error;

pub(crate) fn ensure_select_statement(statement: &Statement) -> PgWireResult<()> {
    match statement {
        Statement::Query(query) if is_select_expr(&query.body) => Ok(()),
        _ => Err(feature_not_supported_error(
            "only SELECT statements are supported",
        )),
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
    let mut parts = Vec::new();
    for part in &name.0 {
        if let Some(Ident { value, .. }) = part.as_ident() {
            parts.push(value.clone());
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

pub(crate) fn is_system_catalog_relation(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized.starts_with("pg_catalog.") || normalized.starts_with("information_schema.")
}

pub(crate) fn unqualified_table_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

#[cfg(test)]
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

#[cfg(test)]
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
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn detects_mv_identifiers_in_sql() {
        let sql = r#"SELECT * FROM mv_orders JOIN "mv_Sales" ON mv_orders.id = "mv_Sales".id"#;
        let mut names = mv_identifiers_in_sql(sql);
        names.sort();
        let mut expected = vec!["mv_orders".to_string(), "mv_Sales".to_string()];
        expected.sort();
        assert_eq!(names, expected);
    }

    #[test]
    fn extracts_schema_qualified_table_names() {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(
            &dialect,
            "SELECT * FROM pg_catalog.pg_matviews UNION ALL SELECT * FROM public.mv_orders",
        )
        .expect("parse");
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let mut names = Vec::new();
        extract_tables_from_query(query, &mut names);
        names.sort();
        assert_eq!(
            names,
            vec![
                "pg_catalog.pg_matviews".to_string(),
                "public.mv_orders".to_string()
            ]
        );
        assert!(is_system_catalog_relation("pg_catalog.pg_matviews"));
        assert_eq!(unqualified_table_name("public.mv_orders"), "mv_orders");
    }
}
