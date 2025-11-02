use anyhow::{Result, anyhow};

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
}
