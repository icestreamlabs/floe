use anyhow::{Result, anyhow};

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
        let definition = parse_materialized_view(sql)?;
        return Ok(FloeStatement::CreateMaterializedView(definition));
    }
    if starts_with_keyword(normalized, "TAIL") {
        return parse_tail_statement(normalized);
    }
    Err(anyhow!("unsupported SQL statement: {normalized}"))
}

pub fn parse_materialized_view(sql: &str) -> Result<MaterializedViewDefinition> {
    let mut rest = sql.trim();
    if rest.is_empty() {
        return Err(anyhow!("materialized view definition cannot be empty"));
    }

    rest = rest.trim_start_matches(|c: char| c.is_ascii_control());
    rest = rest.trim_end_matches(|c: char| c.is_whitespace() || c == ';');

    rest = consume_keyword(rest, "CREATE")
        .ok_or_else(|| anyhow!("expected CREATE at start of materialized view"))?;
    rest = consume_keyword(rest, "MATERIALIZED")
        .ok_or_else(|| anyhow!("expected MATERIALIZED after CREATE"))?;
    rest = consume_keyword(rest, "VIEW")
        .ok_or_else(|| anyhow!("expected VIEW after CREATE MATERIALIZED"))?;

    let mut if_not_exists = false;
    if let Some(next) = consume_sequence(rest, &["IF", "NOT", "EXISTS"]) {
        if_not_exists = true;
        rest = next;
    }

    let (next, name) = parse_identifier(rest)?;
    rest = next;

    let trimmed = rest.trim_start();
    if starts_with_keyword(trimmed, "WITH") {
        return Err(anyhow!(
            "WITH clause for materialized views is not supported yet"
        ));
    }

    rest = consume_keyword(rest, "AS")
        .ok_or_else(|| anyhow!("expected AS clause in materialized view"))?;

    let query = rest.trim();
    if query.is_empty() {
        return Err(anyhow!("materialized view requires a SELECT query"));
    }

    let query = trim_query(query)?;

    Ok(MaterializedViewDefinition {
        name,
        query: query.to_string(),
        if_not_exists,
    })
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

fn trim_query(query: &str) -> Result<&str> {
    let trimmed = query.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if let Some(idx) = trimmed.rfind(';') {
        if trimmed[idx + 1..].trim().is_empty() {
            return Ok(trimmed[..idx].trim_end());
        } else {
            return Err(anyhow!(
                "materialized view definition cannot contain multiple statements"
            ));
        }
    }
    Ok(trimmed)
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
    fn reject_with_clause() {
        let sql = "CREATE MATERIALIZED VIEW mv WITH (foo = 'bar') AS SELECT 1";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(err.to_string().contains("WITH"));
    }

    #[test]
    fn reject_missing_as() {
        let sql = "CREATE MATERIALIZED VIEW mv SELECT 1";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(err.to_string().contains("AS"));
    }

    #[test]
    fn reject_empty_query() {
        let sql = "CREATE MATERIALIZED VIEW mv AS";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(err.to_string().contains("SELECT"));
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
