use anyhow::{Result, anyhow};
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FloeStatement {
    CreateMaterializedView(MaterializedViewDefinition),
    Tail {
        mv_name: String,
        with_snapshot: bool,
        as_of: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    name: String,
    query: String,
    if_not_exists: bool,
}

impl MaterializedViewDefinition {
    pub fn new(name: impl Into<String>, query: impl Into<String>, if_not_exists: bool) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            if_not_exists,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[allow(dead_code)]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[allow(dead_code)]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

pub fn parse_floe_statement(sql: &str) -> Result<FloeStatement> {
    let normalized = normalize_sql(sql)?;
    if starts_with_keyword(normalized, "CREATE") {
        let definition = parse_materialized_view(normalized)?;
        return Ok(FloeStatement::CreateMaterializedView(definition));
    }
    if starts_with_keyword(normalized, "TAIL") {
        return parse_tail_statement(normalized);
    }
    Err(anyhow!("unsupported SQL statement: {normalized}"))
}

pub fn parse_materialized_view(sql: &str) -> Result<MaterializedViewDefinition> {
    let normalized = normalize_sql(sql)?;
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, normalized)
        .map_err(|err| anyhow!("failed to parse materialized view statement: {err}"))?;

    if statements.is_empty() {
        return Err(anyhow!("materialized view definition cannot be empty"));
    }
    if statements.len() != 1 {
        return Err(anyhow!(
            "materialized view definition cannot contain multiple statements"
        ));
    }

    let statement = statements.remove(0);
    match statement {
        Statement::CreateView {
            or_alter,
            or_replace,
            materialized,
            name,
            columns,
            query,
            options: _,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
            params,
        } => {
            if !materialized {
                return Err(anyhow!("expected CREATE MATERIALIZED VIEW statement"));
            }
            if or_alter || or_replace || temporary {
                return Err(anyhow!(
                    "CREATE MATERIALIZED VIEW does not support OR ALTER/REPLACE or TEMPORARY"
                ));
            }
            if !columns.is_empty() {
                return Err(anyhow!(
                    "column lists are not supported in materialized view definitions"
                ));
            }
            if !cluster_by.is_empty()
                || comment.is_some()
                || with_no_schema_binding
                || to.is_some()
                || params.is_some()
            {
                return Err(anyhow!(
                    "unsupported CREATE MATERIALIZED VIEW options are present"
                ));
            }

            let name = object_name_to_string(&name)?;
            let query = query.to_string();
            if query.trim().is_empty() {
                return Err(anyhow!("materialized view requires a SELECT query"));
            }

            Ok(MaterializedViewDefinition {
                name,
                query,
                if_not_exists,
            })
        }
        _ => Err(anyhow!("expected CREATE MATERIALIZED VIEW statement")),
    }
}

fn parse_tail_statement(sql: &str) -> Result<FloeStatement> {
    let mut rest = consume_keyword(sql, "TAIL")
        .ok_or_else(|| anyhow!("expected TAIL at start of statement"))?;
    let (next, mv_name) = parse_identifier(rest)?;
    rest = next;

    let mut with_snapshot = false;
    if let Some(next) = consume_sequence(rest, &["WITH", "SNAPSHOT"]) {
        with_snapshot = true;
        rest = next;
    }

    let mut as_of = None;
    rest = rest.trim_start();
    if !rest.is_empty() {
        let Some(after_as) = consume_keyword(rest, "AS") else {
            return Err(anyhow!(
                "unexpected tokens after TAIL statement: {}",
                rest.trim()
            ));
        };
        let after_of = consume_keyword(after_as, "OF")
            .ok_or_else(|| anyhow!("expected OF after AS in TAIL statement"))?;
        let (next, version) = parse_integer_literal(after_of)?;
        as_of = Some(version);
        rest = next;
    }

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after TAIL statement: {}",
            rest.trim()
        ));
    }

    Ok(FloeStatement::Tail {
        mv_name,
        with_snapshot,
        as_of,
    })
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    let (candidate, rest) = trimmed.split_at(keyword.len());
    if !candidate.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if !is_keyword_boundary(rest.chars().next()) {
        return None;
    }
    Some(rest)
}

fn consume_sequence<'a>(mut input: &'a str, keywords: &[&str]) -> Option<&'a str> {
    for keyword in keywords {
        input = consume_keyword(input, keyword)?;
    }
    Some(input)
}

fn is_keyword_boundary(ch: Option<char>) -> bool {
    match ch {
        None => true,
        Some(c) => c.is_whitespace() || matches!(c, '(' | ')' | ';' | ','),
    }
}

fn parse_identifier(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(anyhow!("expected identifier"));
    }

    if trimmed.starts_with('"') {
        parse_quoted_identifier(trimmed)
    } else {
        parse_unquoted_identifier(trimmed)
    }
}

fn parse_quoted_identifier(input: &str) -> Result<(&str, String)> {
    let bytes = input.as_bytes();
    let mut value = String::new();
    let mut i = 1; // Skip opening quote

    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    value.push('"');
                    i += 2;
                } else {
                    let rest = &input[i + 1..];
                    return Ok((rest, value));
                }
            }
            ch => {
                value.push(ch as char);
                i += 1;
            }
        }
    }

    Err(anyhow!("unterminated quoted identifier"))
}

fn parse_unquoted_identifier(input: &str) -> Result<(&str, String)> {
    let mut end = 0;
    for byte in input.as_bytes() {
        let ch = *byte as char;
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            end += 1;
        } else {
            break;
        }
    }

    if end == 0 {
        return Err(anyhow!("expected identifier"));
    }

    let name = &input[..end];
    let rest = &input[end..];
    Ok((rest, name.to_string()))
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    consume_keyword(input, keyword).is_some()
}

fn object_name_to_string(name: &ObjectName) -> Result<String> {
    let mut parts = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        let ident = part.as_ident().ok_or_else(|| {
            anyhow!("materialized view name contains unsupported identifier syntax")
        })?;
        parts.push(ident.value.as_str());
    }
    Ok(parts.join("."))
}

fn normalize_sql(sql: &str) -> Result<&str> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("SQL statement cannot be empty"));
    }
    let trimmed = trimmed.trim_start_matches(|c: char| c.is_ascii_control());
    let trimmed = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("SQL statement cannot be empty"));
    }
    Ok(trimmed)
}

fn parse_integer_literal(input: &str) -> Result<(&str, i64)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(anyhow!("expected integer literal"));
    }
    let bytes = trimmed.as_bytes();
    let mut end = 0;
    if matches!(bytes[0], b'+' | b'-') {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == 0 || (end == 1 && matches!(bytes[0], b'+' | b'-')) {
        return Err(anyhow!("expected integer literal"));
    }
    let literal = &trimmed[..end];
    let value = literal
        .parse::<i64>()
        .map_err(|_| anyhow!("integer literal '{literal}' is out of range"))?;
    Ok((&trimmed[end..], value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_person";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "mv");
        assert_eq!(mv.query, "SELECT * FROM nexmark_person");
        assert!(!mv.if_not_exists);
    }

    #[test]
    fn parse_if_not_exists() {
        let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert!(mv.if_not_exists);
        assert_eq!(mv.query, "SELECT 1");
    }

    #[test]
    fn parse_with_clause() {
        let sql = "CREATE MATERIALIZED VIEW mv WITH (foo = 'bar') AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "mv");
        assert_eq!(mv.query, "SELECT 1");
    }

    #[test]
    fn reject_missing_as() {
        let sql = "CREATE MATERIALIZED VIEW mv SELECT 1";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to parse materialized view statement")
        );
    }

    #[test]
    fn reject_empty_query() {
        let sql = "CREATE MATERIALIZED VIEW mv AS";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to parse materialized view statement")
        );
    }

    #[test]
    fn reject_multiple_statements() {
        let sql =
            "CREATE MATERIALIZED VIEW mv AS SELECT 1; CREATE MATERIALIZED VIEW mv2 AS SELECT 2";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(err.to_string().contains("multiple statements"));
    }

    #[test]
    fn parse_quoted_identifier() {
        let sql = "CREATE MATERIALIZED VIEW \"MyView\" AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "MyView");
    }

    #[test]
    fn parse_tail_variants() {
        let stmt = parse_floe_statement("TAIL mv_orders").expect("parse tail");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: false,
                as_of: None,
            }
        );

        let stmt =
            parse_floe_statement("TAIL mv_orders WITH SNAPSHOT").expect("parse tail snapshot");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: true,
                as_of: None,
            }
        );

        let stmt = parse_floe_statement("TAIL mv_orders AS OF 42").expect("parse tail as of");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: false,
                as_of: Some(42),
            }
        );

        let stmt = parse_floe_statement("TAIL mv_orders WITH SNAPSHOT AS OF 42")
            .expect("parse tail snapshot as of");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: true,
                as_of: Some(42),
            }
        );
    }
}
